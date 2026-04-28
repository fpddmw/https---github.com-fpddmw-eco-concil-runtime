#!/usr/bin/env python3
"""Review DB-backed evidence sufficiency without becoming a phase gate."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "review-evidence-sufficiency"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.analysis_objects import (  # noqa: E402
    WP4_DECISION_SOURCE_APPROVED_HELPER_VIEW,
    wp4_helper_metadata,
)
from eco_council_runtime.council_objects import query_council_objects  # noqa: E402
from eco_council_runtime.reporting_objects import query_reporting_objects  # noqa: E402


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def resolve_run_dir(run_dir: str) -> Path:
    return Path(run_dir).expanduser().resolve()


def resolve_path(run_dir: Path, path_text: str, default_name: str) -> Path:
    text = maybe_text(path_text)
    if not text:
        return (run_dir / "analytics" / default_name).resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def list_items(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def dict_items(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def unique_values(values: list[Any]) -> list[Any]:
    seen: set[str] = set()
    results: list[Any] = []
    for value in values:
        try:
            key = json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        except TypeError:
            key = maybe_text(value)
        if not key or key in seen:
            continue
        seen.add(key)
        results.append(value)
    return results


def unique_texts(values: list[Any]) -> list[str]:
    return [maybe_text(value) for value in unique_values(values) if maybe_text(value)]


def artifact_ref(path: Path, locator: str = "$") -> dict[str, str]:
    return {
        "signal_id": "",
        "artifact_path": str(path),
        "record_locator": locator,
        "artifact_ref": f"{path}:{locator}",
    }


def object_id_for(kind: str, item: dict[str, Any]) -> str:
    id_fields = {
        "finding": "finding_id",
        "evidence-bundle": "bundle_id",
        "review-comment": "comment_id",
        "report-section-draft": "section_id",
    }
    return maybe_text(item.get(id_fields.get(kind, "")))


def evidence_refs_for(item: dict[str, Any]) -> list[Any]:
    refs = list_items(item.get("evidence_refs"))
    if refs:
        return refs
    basis_refs = list_items(item.get("basis_refs"))
    if basis_refs:
        return basis_refs
    published_refs = list_items(item.get("published_report_refs"))
    return published_refs


def basis_ids_for(item: dict[str, Any]) -> list[str]:
    values: list[Any] = []
    for field_name in (
        "basis_object_ids",
        "source_signal_ids",
        "finding_ids",
        "linked_bundle_ids",
        "response_to_ids",
        "related_object_ids",
        "accepted_object_ids",
        "rejected_object_ids",
    ):
        values.extend(list_items(item.get(field_name)))
    return unique_texts(values)


def object_text(item: dict[str, Any]) -> str:
    fields = [
        "title",
        "summary",
        "rationale",
        "comment_text",
        "section_title",
        "section_text",
        "body",
    ]
    return " ".join(maybe_text(item.get(field)) for field in fields if maybe_text(item.get(field)))


def load_council_kind(
    run_dir: Path,
    *,
    object_kind: str,
    run_id: str,
    round_id: str,
    target_kind: str,
    target_id: str,
    limit: int,
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "object_kind": object_kind,
        "run_id": run_id,
        "round_id": round_id,
        "limit": limit,
    }
    if maybe_text(target_kind):
        kwargs["target_kind"] = maybe_text(target_kind)
    if maybe_text(target_id):
        kwargs["target_id"] = maybe_text(target_id)
    return query_council_objects(run_dir, **kwargs)


def load_reporting_sections(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    limit: int,
) -> dict[str, Any]:
    return query_reporting_objects(
        run_dir,
        object_kind="report-section-draft",
        run_id=run_id,
        round_id=round_id,
        limit=limit,
    )


def object_index(
    *,
    findings: list[dict[str, Any]],
    bundles: list[dict[str, Any]],
    comments: list[dict[str, Any]],
    report_sections: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    return {
        "finding": findings,
        "evidence-bundle": bundles,
        "review-comment": comments,
        "report-section-draft": report_sections,
    }


def collect_refs(objects: list[dict[str, Any]]) -> list[Any]:
    refs: list[Any] = []
    for item in objects:
        refs.extend(evidence_refs_for(item))
    return unique_values(refs)


def collect_ids(kind: str, objects: list[dict[str, Any]]) -> list[str]:
    return unique_texts([object_id_for(kind, item) for item in objects])


def note_payload(
    *,
    run_id: str,
    round_id: str,
    dimension: str,
    review_status: str,
    summary: str,
    supporting_object_ids: list[str],
    evidence_refs: list[Any],
    lineage: list[str],
    provenance: dict[str, Any],
    caveats: list[str] | None = None,
) -> dict[str, Any]:
    note_id = "suffnote-" + stable_hash(
        run_id,
        round_id,
        dimension,
        review_status,
        summary,
        ",".join(supporting_object_ids),
    )[:12]
    return {
        "note_id": note_id,
        "run_id": run_id,
        "round_id": round_id,
        "dimension": dimension,
        "review_status": review_status,
        "summary": summary,
        "supporting_object_ids": unique_texts(supporting_object_ids),
        "evidence_refs": unique_values(evidence_refs),
        "lineage": unique_texts(lineage),
        "provenance": dict_items(provenance),
        "caveats": unique_texts(caveats or []),
    }


def build_review_notes(
    *,
    run_id: str,
    round_id: str,
    findings: list[dict[str, Any]],
    bundles: list[dict[str, Any]],
    comments: list[dict[str, Any]],
    report_sections: list[dict[str, Any]],
    provenance: dict[str, Any],
) -> list[dict[str, Any]]:
    finding_ids = collect_ids("finding", findings)
    bundle_ids = collect_ids("evidence-bundle", bundles)
    comment_ids = collect_ids("review-comment", comments)
    section_ids = collect_ids("report-section-draft", report_sections)
    basis_objects = [*findings, *bundles]
    all_reviewed = [*findings, *bundles, *comments, *report_sections]
    traceable_count = sum(1 for item in all_reviewed if evidence_refs_for(item))
    all_basis_ids: list[str] = []
    for item in all_reviewed:
        all_basis_ids.extend(basis_ids_for(item))
    uncertainty_terms = ("uncertain", "uncertainty", "unknown", "limited", "gap", "missing")
    uncertainty_objects = [
        item
        for item in all_reviewed
        if any(term in object_text(item).casefold() for term in uncertainty_terms)
    ]
    open_comments = [
        item for item in comments if maybe_text(item.get("status")).casefold() in {"open", "submitted"}
    ]
    report_sections_with_basis = [
        item
        for item in report_sections
        if evidence_refs_for(item) or basis_ids_for(item)
    ]

    notes: list[dict[str, Any]] = []
    notes.append(
        note_payload(
            run_id=run_id,
            round_id=round_id,
            dimension="evidence-basis-presence",
            review_status="documented" if basis_objects else "missing-input",
            summary=(
                f"{len(findings)} findings and {len(bundles)} evidence bundles are available for review."
                if basis_objects
                else "No DB-backed findings or evidence bundles were available for review."
            ),
            supporting_object_ids=[*finding_ids, *bundle_ids],
            evidence_refs=collect_refs(basis_objects),
            lineage=[*finding_ids, *bundle_ids],
            provenance=provenance,
            caveats=[
                "This presence check does not judge factual truth or policy adequacy.",
            ],
        )
    )
    notes.append(
        note_payload(
            run_id=run_id,
            round_id=round_id,
            dimension="source-traceability",
            review_status=(
                "documented"
                if all_reviewed and traceable_count == len(all_reviewed)
                else "attention-needed"
                if all_reviewed
                else "missing-input"
            ),
            summary=(
                f"{traceable_count} of {len(all_reviewed)} reviewed objects carry evidence refs."
                if all_reviewed
                else "No reviewed objects were available for traceability review."
            ),
            supporting_object_ids=[
                *finding_ids,
                *bundle_ids,
                *comment_ids,
                *section_ids,
            ],
            evidence_refs=collect_refs(all_reviewed),
            lineage=[
                *finding_ids,
                *bundle_ids,
                *comment_ids,
                *section_ids,
                *all_basis_ids,
            ],
            provenance=provenance,
            caveats=[
                "Evidence refs identify cited basis objects; they do not establish source representativeness."
            ],
        )
    )
    notes.append(
        note_payload(
            run_id=run_id,
            round_id=round_id,
            dimension="counter-evidence-and-challenge",
            review_status=(
                "contested"
                if open_comments
                else "documented"
                if comments
                else "attention-needed"
            ),
            summary=(
                f"{len(open_comments)} open challenger review comments remain."
                if open_comments
                else f"{len(comments)} challenger review comments are available."
                if comments
                else "No challenger review comments were available; counter-evidence may be under-reviewed."
            ),
            supporting_object_ids=comment_ids,
            evidence_refs=collect_refs(comments),
            lineage=comment_ids,
            provenance=provenance,
            caveats=[
                "Absence of review comments is not evidence that counter-evidence is absent."
            ],
        )
    )
    notes.append(
        note_payload(
            run_id=run_id,
            round_id=round_id,
            dimension="report-basis-alignment",
            review_status=(
                "documented"
                if report_sections_with_basis
                else "attention-needed"
                if report_sections
                else "missing-input"
            ),
            summary=(
                f"{len(report_sections_with_basis)} of {len(report_sections)} report section drafts cite basis objects or evidence refs."
                if report_sections
                else "No report section draft basis was available for sufficiency review."
            ),
            supporting_object_ids=section_ids,
            evidence_refs=collect_refs(report_sections),
            lineage=[*section_ids, *all_basis_ids],
            provenance=provenance,
            caveats=[
                "Report sections must cite frozen basis objects before this helper can support report use."
            ],
        )
    )
    notes.append(
        note_payload(
            run_id=run_id,
            round_id=round_id,
            dimension="uncertainty-and-scope",
            review_status="documented" if uncertainty_objects else "attention-needed",
            summary=(
                f"{len(uncertainty_objects)} reviewed objects explicitly mention uncertainty, limits, or gaps."
                if uncertainty_objects
                else "No reviewed objects explicitly mention uncertainty, limits, or gaps."
            ),
            supporting_object_ids=[
                object_id_for(kind, item)
                for kind, items in object_index(
                    findings=findings,
                    bundles=bundles,
                    comments=comments,
                    report_sections=report_sections,
                ).items()
                for item in items
                if item in uncertainty_objects
            ],
            evidence_refs=collect_refs(uncertainty_objects),
            lineage=[
                object_id_for(kind, item)
                for kind, items in object_index(
                    findings=findings,
                    bundles=bundles,
                    comments=comments,
                    report_sections=report_sections,
                ).items()
                for item in items
                if item in uncertainty_objects
            ],
            provenance=provenance,
            caveats=[
                "Uncertainty review is text-based and should be checked by a challenger before report use."
            ],
        )
    )
    return notes


def review_posture(notes: list[dict[str, Any]]) -> str:
    statuses = {maybe_text(note.get("review_status")) for note in notes}
    if "missing-input" in statuses:
        return "insufficient-inputs"
    if "contested" in statuses:
        return "contested-review"
    if "attention-needed" in statuses:
        return "needs-human-review"
    return "documented-with-caveats"


def review_evidence_sufficiency_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    target_kind: str,
    target_id: str,
    rubric_version: str,
    output_path: str,
    limit: int,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_path(
        run_dir_path,
        output_path,
        f"evidence_sufficiency_review_{round_id}.json",
    )
    safe_limit = max(1, min(200, int(limit or 100)))
    warnings: list[dict[str, str]] = []
    metadata = wp4_helper_metadata(
        skill_name=SKILL_NAME,
        rule_id="HEUR-SUFFICIENCY-REVIEW-001",
        destination="DB-backed evidence sufficiency notes and caveats",
        decision_source=WP4_DECISION_SOURCE_APPROVED_HELPER_VIEW,
        rubric_version=maybe_text(rubric_version) or "wp4-sufficiency-rubric-v0.1",
        approval_ref="required:skill_approval_request",
        audit_ref="docs/openclaw-wp4-skills-refactor-workplan.md#8",
        rule_trace=["db-council-object-presence-review", "report-basis-citation-review"],
        caveats=[
            "This helper does not score readiness or prove claims.",
            "Report use requires explicit citation through DB council or reporting basis objects.",
        ],
        audit_status="default-frozen; approval-required; audit-pending",
        helper_status="approval-gated-helper-view",
    )
    finding_query = load_council_kind(
        run_dir_path,
        object_kind="finding",
        run_id=run_id,
        round_id=round_id,
        target_kind=target_kind,
        target_id=target_id,
        limit=safe_limit,
    )
    bundle_query = load_council_kind(
        run_dir_path,
        object_kind="evidence-bundle",
        run_id=run_id,
        round_id=round_id,
        target_kind=target_kind,
        target_id=target_id,
        limit=safe_limit,
    )
    comment_query = load_council_kind(
        run_dir_path,
        object_kind="review-comment",
        run_id=run_id,
        round_id=round_id,
        target_kind=target_kind,
        target_id=target_id,
        limit=safe_limit,
    )
    section_query = load_reporting_sections(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        limit=safe_limit,
    )
    findings = list_items(finding_query.get("objects"))
    bundles = list_items(bundle_query.get("objects"))
    comments = list_items(comment_query.get("objects"))
    report_sections = list_items(section_query.get("objects"))
    provenance = {
        "source_skill": SKILL_NAME,
        "decision_source": metadata["decision_source"],
        "rule_id": metadata["rule_id"],
        "rule_version": metadata["rule_version"],
        "rubric_version": metadata["rubric_version"],
        "artifact_path": str(output_file),
        "authoritative_inputs": [
            "finding_records",
            "evidence_bundles",
            "review_comments",
            "report_section_drafts",
        ],
    }
    notes = build_review_notes(
        run_id=run_id,
        round_id=round_id,
        findings=findings,
        bundles=bundles,
        comments=comments,
        report_sections=report_sections,
        provenance=provenance,
    )
    if not findings and not bundles:
        warnings.append(
            {
                "code": "missing-db-evidence-basis",
                "message": "No DB-backed findings or evidence bundles were available for sufficiency review.",
            }
        )
    if not comments:
        warnings.append(
            {
                "code": "missing-challenger-review",
                "message": "No challenger review comments were available; counter-evidence review is incomplete.",
            }
        )
    if not report_sections:
        warnings.append(
            {
                "code": "missing-report-section-basis",
                "message": "No report section drafts were available for report-basis alignment review.",
            }
        )
    reviewed_ids = unique_texts(
        [
            *collect_ids("finding", findings),
            *collect_ids("evidence-bundle", bundles),
            *collect_ids("review-comment", comments),
            *collect_ids("report-section-draft", report_sections),
        ]
    )
    review = {
        "review_id": "suffreview-" + stable_hash(run_id, round_id, target_kind, target_id, output_file)[:12],
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "wp4_helper_metadata": metadata,
        "target": {
            "target_kind": maybe_text(target_kind),
            "target_id": maybe_text(target_id),
        },
        "rubric": {
            "rubric_version": metadata["rubric_version"],
            "output_mode": "notes-and-caveats",
            "numeric_scores": False,
            "phase_gate": False,
        },
        "review_posture": review_posture(notes),
        "reviewed_object_counts": {
            "findings": len(findings),
            "evidence_bundles": len(bundles),
            "review_comments": len(comments),
            "report_section_drafts": len(report_sections),
        },
        "reviewed_object_ids": reviewed_ids,
        "notes": notes,
        "gaps": [
            note["summary"]
            for note in notes
            if maybe_text(note.get("review_status")) in {"missing-input", "attention-needed"}
        ],
        "counter_evidence_notes": [
            note
            for note in notes
            if maybe_text(note.get("dimension")) == "counter-evidence-and-challenge"
        ],
        "uncertainty_notes": [
            note
            for note in notes
            if maybe_text(note.get("dimension")) == "uncertainty-and-scope"
        ],
        "report_usage_constraints": [
            "This helper output is advisory and cannot be cited as report basis unless a DB council object or report basis explicitly references it.",
            "This helper does not decide phase transition, publication, or claim truth.",
        ],
        "lineage": reviewed_ids,
        "provenance": provenance,
        "evidence_refs": collect_refs([*findings, *bundles, *comments, *report_sections]),
    }
    wrapper = {
        "schema_version": "wp4-evidence-sufficiency-review-v1",
        "skill": SKILL_NAME,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": review["generated_at_utc"],
        "status": "completed",
        "query_basis": {
            "db_path": maybe_text(finding_query.get("summary", {}).get("db_path")),
            "target_kind": maybe_text(target_kind),
            "target_id": maybe_text(target_id),
            "limit": safe_limit,
            "input_surfaces": [
                "finding",
                "evidence-bundle",
                "review-comment",
                "report-section-draft",
            ],
        },
        "observed_inputs": {
            "finding_query": finding_query.get("summary", {}),
            "evidence_bundle_query": bundle_query.get("summary", {}),
            "review_comment_query": comment_query.get("summary", {}),
            "report_section_query": section_query.get("summary", {}),
        },
        "review": review,
        "warnings": warnings,
    }
    write_json(output_file, wrapper)
    ref = artifact_ref(output_file, "$.review")
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "review_id": review["review_id"],
            "review_posture": review["review_posture"],
            "note_count": len(notes),
            "reviewed_object_count": len(reviewed_ids),
            "decision_source": metadata["decision_source"],
            "rule_id": metadata["rule_id"],
            "rubric_version": metadata["rubric_version"],
        },
        "receipt_id": "suffreview-receipt-"
        + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:20],
        "batch_id": "suffreview-batch-"
        + stable_hash(SKILL_NAME, run_id, round_id, "wp4")[:16],
        "artifact_refs": [ref],
        "canonical_ids": [review["review_id"]],
        "warnings": warnings,
        "review": review,
        "board_handoff": {
            "candidate_ids": [review["review_id"]],
            "evidence_refs": [ref],
            "gap_hints": review["gaps"],
            "challenge_hints": [
                "Challenger should review sufficiency rubric, source coverage, uncertainty handling, and report usage before any report-basis citation."
            ],
            "suggested_next_skills": [],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Review DB-backed evidence sufficiency without numeric readiness scoring."
    )
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--target-kind", default="")
    parser.add_argument("--target-id", default="")
    parser.add_argument("--rubric-version", default="wp4-sufficiency-rubric-v0.1")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = review_evidence_sufficiency_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        target_kind=args.target_kind,
        target_id=args.target_id,
        rubric_version=args.rubric_version,
        output_path=args.output_path,
        limit=args.limit,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
