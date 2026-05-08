"""Structural report-claim and lead-basis helpers.

This module only reads explicit agent/council reporting objects. It does not
rank evidence, infer claim support, or decide substantive sufficiency.
"""

from __future__ import annotations

from typing import Any


LEAD_BASIS_USE_VALUES = {
    "lead-basis",
    "lead_basis",
    "primary-basis",
    "core-basis",
}


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def list_items(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def unique_texts(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for value in values:
        text = maybe_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        results.append(text)
    return results


def boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return maybe_text(value).casefold() in {"1", "true", "yes", "y", "on"}


def section_declares_lead_basis(section: dict[str, Any]) -> bool:
    basis_use = maybe_text(section.get("basis_use")).casefold()
    return boolish(section.get("lead_basis")) or basis_use in LEAD_BASIS_USE_VALUES


def section_declares_report_claim(section: dict[str, Any]) -> bool:
    if section_declares_lead_basis(section):
        return True
    return any(
        maybe_text(section.get(field_name))
        for field_name in (
            "claim_id",
            "claim_text",
            "basis_use",
        )
    )


def section_reference_ids(section: dict[str, Any]) -> list[str]:
    return unique_texts(
        [
            section.get("section_id"),
            section.get("claim_id"),
            *list_items(section.get("basis_object_ids")),
            *list_items(section.get("bundle_ids")),
            *list_items(section.get("finding_ids")),
            *list_items(section.get("claim_constraint_ids")),
        ]
    )


def section_evidence_refs(section: dict[str, Any]) -> list[str]:
    return unique_texts(list_items(section.get("evidence_refs")))


def lead_basis_object_from_section(section: dict[str, Any]) -> dict[str, Any]:
    section_id = maybe_text(section.get("section_id"))
    claim_text = maybe_text(section.get("claim_text"))
    missing_fields = []
    if not claim_text:
        missing_fields.append("claim_text")
    if not section_evidence_refs(section):
        missing_fields.append("evidence_refs")
    return {
        "lead_basis_id": "lead-basis-" + section_id,
        "source_object_kind": "report-section-draft",
        "source_object_id": section_id,
        "section_id": section_id,
        "section_key": maybe_text(section.get("section_key")),
        "claim_id": maybe_text(section.get("claim_id")),
        "claim_text": claim_text,
        "basis_use": maybe_text(section.get("basis_use")) or "lead-basis",
        "basis_object_ids": unique_texts(list_items(section.get("basis_object_ids"))),
        "bundle_ids": unique_texts(list_items(section.get("bundle_ids"))),
        "finding_ids": unique_texts(list_items(section.get("finding_ids"))),
        "evidence_refs": section_evidence_refs(section),
        "reference_ids": section_reference_ids(section),
        "structural_status": "complete" if not missing_fields else "incomplete",
        "missing_structural_fields": missing_fields,
    }


def report_claim_object_from_section(section: dict[str, Any]) -> dict[str, Any]:
    section_id = maybe_text(section.get("section_id"))
    claim_id = maybe_text(section.get("claim_id")) or "report-claim-" + section_id
    blocking_missing_fields = []
    for field_name in ("claim_text",):
        if not maybe_text(section.get(field_name)):
            blocking_missing_fields.append(field_name)
    if not section_evidence_refs(section):
        blocking_missing_fields.append("evidence_refs")
    return {
        "claim_id": claim_id,
        "source_object_kind": "report-section-draft",
        "source_object_id": section_id,
        "section_id": section_id,
        "section_key": maybe_text(section.get("section_key")),
        "claim_text": maybe_text(section.get("claim_text")),
        "claim_constraint_ids": unique_texts(
            list_items(section.get("claim_constraint_ids"))
        ),
        "basis_use": maybe_text(section.get("basis_use")),
        "basis_object_ids": unique_texts(list_items(section.get("basis_object_ids"))),
        "bundle_ids": unique_texts(list_items(section.get("bundle_ids"))),
        "finding_ids": unique_texts(list_items(section.get("finding_ids"))),
        "evidence_refs": section_evidence_refs(section),
        "reference_ids": section_reference_ids(section),
        "is_lead_basis": section_declares_lead_basis(section),
        "structural_status": "complete"
        if not blocking_missing_fields
        else "incomplete",
        "missing_structural_fields": blocking_missing_fields,
    }


def constraint_matches_lead_basis(
    constraint: dict[str, Any],
    lead_basis: dict[str, Any],
) -> bool:
    reference_ids = set(unique_texts(list_items(lead_basis.get("reference_ids"))))
    target_id = maybe_text(constraint.get("target_id"))
    if target_id and target_id in reference_ids:
        return True
    constraint_refs = set(unique_texts(list_items(constraint.get("evidence_refs"))))
    lead_refs = set(unique_texts(list_items(lead_basis.get("evidence_refs"))))
    return bool(constraint_refs and lead_refs and constraint_refs.intersection(lead_refs))


def constraint_matches_report_claim(
    constraint: dict[str, Any],
    report_claim: dict[str, Any],
) -> bool:
    reference_ids = set(unique_texts(list_items(report_claim.get("reference_ids"))))
    target_id = maybe_text(constraint.get("target_id"))
    if target_id and target_id in reference_ids:
        return True
    constraint_refs = set(unique_texts(list_items(constraint.get("evidence_refs"))))
    claim_refs = set(unique_texts(list_items(report_claim.get("evidence_refs"))))
    return bool(constraint_refs and claim_refs and constraint_refs.intersection(claim_refs))


def report_claim_structural_violations(
    report_claim_objects: list[dict[str, Any]],
    challenger_constraints: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    violations: list[dict[str, Any]] = []
    for report_claim in report_claim_objects:
        if maybe_text(report_claim.get("structural_status")) != "complete":
            violations.append(
                {
                    "violation_id": "report-claim-incomplete-"
                    + maybe_text(report_claim.get("source_object_id")),
                    "violation_kind": "report-claim-incomplete",
                    "claim_id": maybe_text(report_claim.get("claim_id")),
                    "source_object_id": maybe_text(report_claim.get("source_object_id")),
                    "missing_structural_fields": list_items(
                        report_claim.get("missing_structural_fields")
                    ),
                }
            )
        claim_constraint_ids = set(
            unique_texts(list_items(report_claim.get("claim_constraint_ids")))
        )
        for constraint in challenger_constraints:
            if not constraint_matches_report_claim(constraint, report_claim):
                continue
            constraint_id = maybe_text(constraint.get("constraint_id"))
            if bool(constraint.get("release_blocker")):
                violations.append(
                    {
                        "violation_id": "report-claim-unresolved-constraint-"
                        + maybe_text(report_claim.get("source_object_id"))
                        + "-"
                        + constraint_id,
                        "violation_kind": "report-claim-unresolved-constraint",
                        "claim_id": maybe_text(report_claim.get("claim_id")),
                        "source_object_id": maybe_text(
                            report_claim.get("source_object_id")
                        ),
                        "constraint_id": constraint_id,
                        "review_comment_id": maybe_text(
                            constraint.get("review_comment_id")
                        ),
                    }
                )
            elif constraint_id and constraint_id not in claim_constraint_ids:
                violations.append(
                    {
                        "violation_id": "report-claim-missing-constraint-link-"
                        + maybe_text(report_claim.get("source_object_id"))
                        + "-"
                        + constraint_id,
                        "violation_kind": "report-claim-missing-constraint-disposition-link",
                        "claim_id": maybe_text(report_claim.get("claim_id")),
                        "source_object_id": maybe_text(
                            report_claim.get("source_object_id")
                        ),
                        "constraint_id": constraint_id,
                        "review_comment_id": maybe_text(
                            constraint.get("review_comment_id")
                        ),
                        "constraint_disposition": maybe_text(
                            constraint.get("disposition")
                        ),
                    }
                )
    return violations


def lead_basis_constraint_violations(
    lead_basis_objects: list[dict[str, Any]],
    challenger_constraints: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    violations: list[dict[str, Any]] = []
    for lead_basis in lead_basis_objects:
        if maybe_text(lead_basis.get("structural_status")) != "complete":
            violations.append(
                {
                    "violation_id": "lead-basis-incomplete-"
                    + maybe_text(lead_basis.get("source_object_id")),
                    "violation_kind": "lead-basis-incomplete",
                    "lead_basis_id": maybe_text(lead_basis.get("lead_basis_id")),
                    "source_object_id": maybe_text(lead_basis.get("source_object_id")),
                    "missing_structural_fields": list_items(
                        lead_basis.get("missing_structural_fields")
                    ),
                }
            )
        for constraint in challenger_constraints:
            if bool(constraint.get("lead_basis_allowed")):
                continue
            if not constraint_matches_lead_basis(constraint, lead_basis):
                continue
            violations.append(
                {
                    "violation_id": "lead-basis-constraint-"
                    + maybe_text(lead_basis.get("source_object_id"))
                    + "-"
                    + maybe_text(constraint.get("constraint_id")),
                    "violation_kind": "lead-basis-disallowed-by-constraint",
                    "lead_basis_id": maybe_text(lead_basis.get("lead_basis_id")),
                    "source_object_id": maybe_text(lead_basis.get("source_object_id")),
                    "constraint_id": maybe_text(constraint.get("constraint_id")),
                    "review_comment_id": maybe_text(constraint.get("review_comment_id")),
                    "constraint_disposition": maybe_text(constraint.get("disposition")),
                    "basis_use": maybe_text(constraint.get("basis_use")),
                }
            )
    return violations


def report_claim_structure_state(
    report_section_drafts: list[dict[str, Any]],
    *,
    challenger_constraints: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    lead_basis_objects = [
        lead_basis_object_from_section(section)
        for section in report_section_drafts
        if isinstance(section, dict) and section_declares_lead_basis(section)
    ]
    report_claim_objects = [
        report_claim_object_from_section(section)
        for section in report_section_drafts
        if isinstance(section, dict) and section_declares_report_claim(section)
    ]
    lead_violations = lead_basis_constraint_violations(
        lead_basis_objects,
        challenger_constraints or [],
    )
    claim_violations = report_claim_structural_violations(
        report_claim_objects,
        challenger_constraints or [],
    )
    return {
        "explicit_report_claim_count": len(report_claim_objects),
        "explicit_report_claim_objects": report_claim_objects,
        "report_claim_structural_violation_count": len(claim_violations),
        "report_claim_structural_violations": claim_violations,
        "explicit_lead_basis_count": len(lead_basis_objects),
        "explicit_lead_basis_objects": lead_basis_objects,
        "lead_basis_constraint_violation_count": len(lead_violations),
        "lead_basis_constraint_violations": lead_violations,
    }
