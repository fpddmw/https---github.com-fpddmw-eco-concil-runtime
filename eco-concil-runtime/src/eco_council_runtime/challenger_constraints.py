"""Structural handling for challenger report-use constraints.

The helpers in this module do not decide whether evidence supports a claim.
They only turn explicit challenger review comments and explicit disposition
comments into gate state that downstream skills can enforce.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.objects.council import query_council_objects

OPEN_REVIEW_COMMENT_STATUSES = {"", "open", "submitted"}
NON_BLOCKING_REPORT_RISKS = {
    "",
    "none",
    "no-risk",
    "no-report-risk",
    "informational",
}
CONSTRAINT_DISPOSITIONS = {
    "accepted_as_limitation",
    "requires_followup",
    "excluded_from_report_basis",
    "resolved_by_followup",
    "waived_by_challenger",
}
RESOLVED_CONSTRAINT_DISPOSITIONS = {
    "accepted_as_limitation",
    "excluded_from_report_basis",
    "resolved_by_followup",
    "waived_by_challenger",
}
LEAD_BASIS_RESTRICTING_DISPOSITIONS = {
    "",
    "accepted_as_limitation",
    "excluded_from_report_basis",
    "requires_followup",
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


def review_comment_is_open(comment: dict[str, Any]) -> bool:
    return maybe_text(comment.get("status")).casefold() in OPEN_REVIEW_COMMENT_STATUSES


def challenger_comment_requires_constraint(comment: dict[str, Any]) -> bool:
    if maybe_text(comment.get("author_role")) != "challenger":
        return False
    if not review_comment_is_open(comment):
        return False
    report_risk = maybe_text(comment.get("report_risk")).casefold()
    if report_risk and report_risk not in NON_BLOCKING_REPORT_RISKS:
        return True
    required_followup = comment.get("required_followup_evidence", [])
    if isinstance(required_followup, list) and any(
        maybe_text(item) for item in required_followup
    ):
        return True
    return False


def comment_id(comment: dict[str, Any]) -> str:
    return maybe_text(comment.get("comment_id") or comment.get("object_id"))


def comment_references(response: dict[str, Any], target_comment_id: str) -> bool:
    if not target_comment_id:
        return False
    referenced_values: list[Any] = []
    for field_name in (
        "response_to_ids",
        "lineage",
        "basis_object_ids",
        "evidence_refs",
    ):
        referenced_values.extend(list_items(response.get(field_name)))
    referenced_ids = {maybe_text(value) for value in referenced_values}
    if target_comment_id in referenced_ids:
        return True
    thread_id = maybe_text(response.get("thread_id"))
    return bool(thread_id and thread_id == target_comment_id)


def normalized_constraint_disposition(comment: dict[str, Any]) -> str:
    disposition = maybe_text(comment.get("constraint_disposition")).casefold()
    if disposition in CONSTRAINT_DISPOSITIONS:
        return disposition
    return ""


def disposition_comments_for_constraint(
    constraint_comment: dict[str, Any],
    comments: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    target_comment_id = comment_id(constraint_comment)
    if not target_comment_id:
        return []
    results: list[dict[str, Any]] = []
    for candidate in comments:
        if comment_id(candidate) == target_comment_id:
            continue
        disposition = normalized_constraint_disposition(candidate)
        if not disposition:
            continue
        if not comment_references(candidate, target_comment_id):
            continue
        if (
            disposition == "waived_by_challenger"
            and maybe_text(candidate.get("author_role")) != "challenger"
        ):
            continue
        results.append(candidate)
    return results


def latest_disposition_comment(
    constraint_comment: dict[str, Any],
    comments: list[dict[str, Any]],
) -> dict[str, Any] | None:
    candidates = disposition_comments_for_constraint(constraint_comment, comments)
    if not candidates:
        return None
    return candidates[-1]


def basis_use_for_disposition(disposition: str) -> str:
    if disposition == "accepted_as_limitation":
        return "limited-context"
    if disposition == "excluded_from_report_basis":
        return "excluded"
    if disposition in {"resolved_by_followup", "waived_by_challenger"}:
        return "resolved"
    if disposition == "requires_followup":
        return "requires-followup"
    return "unresolved"


def constraint_from_comment(
    constraint_comment: dict[str, Any],
    *,
    disposition_comment: dict[str, Any] | None,
) -> dict[str, Any]:
    source_comment_id = comment_id(constraint_comment)
    disposition = (
        normalized_constraint_disposition(disposition_comment)
        if isinstance(disposition_comment, dict)
        else ""
    )
    is_resolved = disposition in RESOLVED_CONSTRAINT_DISPOSITIONS
    release_blocker = not is_resolved
    lead_basis_allowed = disposition not in LEAD_BASIS_RESTRICTING_DISPOSITIONS
    return {
        "constraint_id": f"challenger-constraint-{source_comment_id}",
        "review_comment_id": source_comment_id,
        "review_kind": maybe_text(constraint_comment.get("review_kind")),
        "target_kind": maybe_text(constraint_comment.get("target_kind")),
        "target_id": maybe_text(constraint_comment.get("target_id")),
        "report_risk": maybe_text(constraint_comment.get("report_risk")),
        "required_followup_evidence": unique_texts(
            list_items(constraint_comment.get("required_followup_evidence"))
        ),
        "evidence_refs": unique_texts(
            list_items(constraint_comment.get("evidence_refs"))
        ),
        "comment_text": maybe_text(constraint_comment.get("comment_text")),
        "disposition": disposition,
        "disposition_comment_id": comment_id(disposition_comment or {}),
        "disposition_author_role": maybe_text(
            (disposition_comment or {}).get("author_role")
        ),
        "status": "resolved" if is_resolved else "unresolved",
        "release_blocker": release_blocker,
        "freeze_blocker": release_blocker,
        "lead_basis_allowed": lead_basis_allowed,
        "basis_use": basis_use_for_disposition(disposition),
    }


def basis_use_constraints_from_constraints(
    constraints: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for constraint in constraints:
        if (
            constraint.get("lead_basis_allowed") is True
            and maybe_text(constraint.get("basis_use")) == "resolved"
        ):
            continue
        results.append(
            {
                "constraint_id": maybe_text(constraint.get("constraint_id")),
                "review_comment_id": maybe_text(constraint.get("review_comment_id")),
                "target_kind": maybe_text(constraint.get("target_kind")),
                "target_id": maybe_text(constraint.get("target_id")),
                "disposition": maybe_text(constraint.get("disposition")),
                "basis_use": maybe_text(constraint.get("basis_use")),
                "lead_basis_allowed": bool(constraint.get("lead_basis_allowed")),
                "release_blocker": bool(constraint.get("release_blocker")),
                "evidence_refs": unique_texts(list_items(constraint.get("evidence_refs"))),
                "comment_text": maybe_text(constraint.get("comment_text")),
            }
        )
    return results


def challenger_constraint_state_from_review_comments(
    comments: list[dict[str, Any]],
) -> dict[str, Any]:
    constraint_comments = [
        comment
        for comment in comments
        if isinstance(comment, dict) and challenger_comment_requires_constraint(comment)
    ]
    constraints = [
        constraint_from_comment(
            comment,
            disposition_comment=latest_disposition_comment(comment, comments),
        )
        for comment in constraint_comments
    ]
    unresolved_constraints = [
        constraint
        for constraint in constraints
        if bool(constraint.get("release_blocker"))
    ]
    return {
        "challenger_constraints": constraints,
        "unresolved_challenger_constraints": unresolved_constraints,
        "challenger_constraint_count": len(constraints),
        "unresolved_challenger_constraint_count": len(unresolved_constraints),
        "challenger_constraint_ids": unique_texts(
            [constraint.get("constraint_id") for constraint in constraints]
        ),
        "unresolved_challenger_constraint_ids": unique_texts(
            [constraint.get("constraint_id") for constraint in unresolved_constraints]
        ),
        "challenger_constraint_review_comment_ids": unique_texts(
            [constraint.get("review_comment_id") for constraint in constraints]
        ),
        "unresolved_challenger_constraint_review_comment_ids": unique_texts(
            [constraint.get("review_comment_id") for constraint in unresolved_constraints]
        ),
        "basis_use_constraints": basis_use_constraints_from_constraints(constraints),
    }


def load_challenger_constraint_state(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
) -> dict[str, Any]:
    payload = query_council_objects(
        run_dir,
        object_kind="review-comment",
        run_id=run_id,
        round_id=round_id,
    )
    objects = (
        payload.get("objects", [])
        if isinstance(payload.get("objects"), list)
        else []
    )
    comments = [comment for comment in objects if isinstance(comment, dict)]
    return challenger_constraint_state_from_review_comments(comments)
