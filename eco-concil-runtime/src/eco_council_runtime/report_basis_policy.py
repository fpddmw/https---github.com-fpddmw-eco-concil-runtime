from __future__ import annotations

from typing import Any


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


def accepted_limitations_from_constraints(
    basis_use_constraints: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    limitations: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, constraint in enumerate(basis_use_constraints, start=1):
        if not isinstance(constraint, dict):
            continue
        disposition = maybe_text(constraint.get("disposition"))
        basis_use = maybe_text(constraint.get("basis_use"))
        if disposition != "accepted_as_limitation" and basis_use != "limited-context":
            continue
        constraint_id = maybe_text(constraint.get("constraint_id"))
        key = "|".join(
            [
                constraint_id,
                maybe_text(constraint.get("review_comment_id")),
                maybe_text(constraint.get("target_kind")),
                maybe_text(constraint.get("target_id")),
                disposition,
            ]
        )
        if key in seen:
            continue
        seen.add(key)
        limitations.append(
            {
                "limitation_id": constraint_id or f"accepted-limitation-{index:03d}",
                "constraint_id": constraint_id,
                "review_comment_id": maybe_text(constraint.get("review_comment_id")),
                "target_kind": maybe_text(constraint.get("target_kind")),
                "target_id": maybe_text(constraint.get("target_id")),
                "disposition": disposition,
                "basis_use": basis_use or "limited-context",
                "summary": maybe_text(constraint.get("comment_text"))
                or "Challenger constraint accepted as a report limitation.",
                "evidence_refs": unique_texts(list_items(constraint.get("evidence_refs"))),
                "report_treatment": (
                    "Carry as an explicit limitation; do not present the target as "
                    "unqualified lead basis."
                ),
            }
        )
    return limitations


def unresolved_challenges_from_constraints(
    unresolved_challenger_constraints: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    challenges: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, constraint in enumerate(unresolved_challenger_constraints, start=1):
        if not isinstance(constraint, dict):
            continue
        constraint_id = maybe_text(constraint.get("constraint_id"))
        key = "|".join(
            [
                constraint_id,
                maybe_text(constraint.get("review_comment_id")),
                maybe_text(constraint.get("target_kind")),
                maybe_text(constraint.get("target_id")),
            ]
        )
        if key in seen:
            continue
        seen.add(key)
        challenges.append(
            {
                "challenge_id": constraint_id or f"unresolved-challenge-{index:03d}",
                "constraint_id": constraint_id,
                "review_comment_id": maybe_text(constraint.get("review_comment_id")),
                "target_kind": maybe_text(constraint.get("target_kind")),
                "target_id": maybe_text(constraint.get("target_id")),
                "status": maybe_text(constraint.get("status")) or "unresolved",
                "summary": maybe_text(constraint.get("comment_text"))
                or "Unresolved challenger constraint blocks report-basis use.",
                "evidence_refs": unique_texts(list_items(constraint.get("evidence_refs"))),
                "report_treatment": (
                    "Do not freeze or publish as resolved until an explicit "
                    "challenge disposition is recorded."
                ),
            }
        )
    return challenges


def report_basis_input_policy() -> dict[str, Any]:
    return {
        "policy_version": "explicit-council-object-report-basis-v1",
        "allowed_report_basis_sources": [
            "finding",
            "evidence-bundle",
            "proposal",
            "readiness-opinion",
            "review-comment",
            "challenge-disposition",
            "report-section-draft",
            "report-basis-freeze",
        ],
        "excluded_direct_sources": [
            "helper-artifact",
            "optional-analysis-result",
            "supervisor-next-action",
            "ranked-action",
            "raw-record",
        ],
        "semantics": (
            "Reporting packets consume explicit council/reporting objects and "
            "frozen evidence refs. Helper artifacts, supervisor next actions, "
            "and query results are audit context only unless cited by explicit "
            "council objects."
        ),
    }


__all__ = [
    "accepted_limitations_from_constraints",
    "report_basis_input_policy",
    "unresolved_challenges_from_constraints",
]
