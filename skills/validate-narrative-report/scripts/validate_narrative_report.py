#!/usr/bin/env python3
"""Validate narrative report draft structure and traceability."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "validate-narrative-report"
REQUIRED_SECTIONS = {
    "executive-summary",
    "key-points",
    "what-happened",
    "evidence-basis",
    "council-reasoning",
    "limitations",
    "decision-implications",
}
MACHINE_PROSE_PREFIXES = (
    "council-decision (",
    "council-decision-draft (",
    "expert-report-",
    "round-synthesis:",
    "agent-position:",
    "finding:",
    "environmental-investigator:",
    "social-investigator:",
)


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


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


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def pretty_json(payload: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def resolve_run_dir(run_dir: str) -> Path:
    return Path(run_dir).expanduser().resolve()


def resolve_path(run_dir: Path, override: str, default_relative: str) -> Path:
    text = maybe_text(override)
    if not text:
        return (run_dir / default_relative).resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def load_json_file(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at {path}.")
    return payload


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def issue(code: str, message: str, severity: str = "warning") -> dict[str, str]:
    return {"code": code, "severity": severity, "message": message}


def validate_draft(draft: dict[str, Any]) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    if maybe_text(draft.get("schema_version")) != "narrative-report-draft-v1":
        issues.append(issue("unexpected-schema", "Draft schema_version is not narrative-report-draft-v1.", "error"))
    boundary = draft.get("claim_boundary") if isinstance(draft.get("claim_boundary"), dict) else {}
    if not maybe_text(boundary.get("summary")):
        issues.append(issue("missing-claim-boundary", "Draft must include a visible claim boundary.", "error"))
    if not isinstance(boundary.get("forbidden_claims"), list) or not boundary["forbidden_claims"]:
        issues.append(issue("missing-forbidden-claims", "Draft must state forbidden claim upgrades.", "warning"))
    sections = draft.get("sections") if isinstance(draft.get("sections"), list) else []
    section_ids = {
        maybe_text(section.get("section_id"))
        for section in sections
        if isinstance(section, dict)
    }
    missing = sorted(REQUIRED_SECTIONS - section_ids)
    for section_id in missing:
        issues.append(issue("missing-section", f"Missing required section: {section_id}.", "error"))
    allowed_ref_optional_statuses = {"limitations-only", "limitations-visible", "boundary-only"}
    for section in sections:
        if not isinstance(section, dict):
            continue
        section_id = maybe_text(section.get("section_id")) or "unknown-section"
        paragraphs = section.get("paragraphs") if isinstance(section.get("paragraphs"), list) else []
        if not any(maybe_text(paragraph) for paragraph in paragraphs):
            issues.append(issue("empty-section", f"Section {section_id} has no paragraph text.", "error"))
        for paragraph in paragraphs:
            text = maybe_text(paragraph)
            lowered = text.lower()
            if any(lowered.startswith(prefix) for prefix in MACHINE_PROSE_PREFIXES):
                issues.append(
                    issue(
                        "machine-object-prose",
                        f"Section {section_id} appears to lead with object labels instead of reader-facing prose.",
                        "warning",
                    )
                )
                break
        refs = section.get("evidence_refs") if isinstance(section.get("evidence_refs"), list) else []
        status = maybe_text(section.get("status"))
        if not refs and status not in allowed_ref_optional_statuses:
            issues.append(issue("section-without-refs", f"Section {section_id} has no evidence refs or limitation status.", "warning"))
    all_paragraphs = [
        maybe_text(paragraph)
        for section in sections
        if isinstance(section, dict)
        for paragraph in (section.get("paragraphs") if isinstance(section.get("paragraphs"), list) else [])
        if maybe_text(paragraph)
    ]
    duplicate_count = len(all_paragraphs) - len(set(all_paragraphs))
    if duplicate_count:
        issues.append(
            issue(
                "duplicate-prose",
                f"Draft repeats {duplicate_count} paragraph(s); narrative reports should explain object roles instead of restating the same artifact text.",
                "warning",
            )
        )
    if any(text.startswith("- ") for text in all_paragraphs):
        issues.append(
            issue(
                "embedded-markdown-bullets",
                "Draft stores Markdown bullet prefixes inside paragraph text; use presentation metadata or plain paragraph strings instead.",
                "warning",
            )
        )
    title = maybe_text(draft.get("title")).lower()
    if title.startswith("narrative report draft for") or title.startswith("narrative report for"):
        issues.append(
            issue(
                "weak-report-title",
                "Report title should identify the subject or basis in reader-facing terms, not only the round id.",
                "warning",
            )
        )
    if not isinstance(draft.get("reader_guidance"), dict):
        issues.append(issue("missing-reader-guidance", "Draft should include reader_guidance describing intended audience and style.", "warning"))
    if not isinstance(draft.get("evidence_refs"), list) or not draft["evidence_refs"]:
        issues.append(issue("missing-evidence-index", "Draft has no top-level evidence_refs index.", "warning"))
    if not isinstance(draft.get("audit_refs"), list) or not draft["audit_refs"]:
        issues.append(issue("missing-audit-refs", "Draft has no audit_refs index.", "warning"))
    return issues


def validate_narrative_report(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    draft_path: str = "",
    output_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    draft_file = resolve_path(run_dir_path, draft_path, f"reporting/narrative_report_draft_{round_id}.json")
    output_file = resolve_path(run_dir_path, output_path, f"reporting/narrative_report_validation_{round_id}.json")
    draft = load_json_file(draft_file)
    issues = validate_draft(draft)
    error_count = sum(1 for item in issues if item.get("severity") == "error")
    warning_count = sum(1 for item in issues if item.get("severity") != "error")
    validation_id = "narrative-report-validation-" + stable_hash(run_id, round_id, draft.get("draft_id"), issues)[:12]
    validation = {
        "schema_version": "narrative-report-validation-v1",
        "validation_id": validation_id,
        "run_id": run_id,
        "round_id": round_id,
        "draft_id": maybe_text(draft.get("draft_id")),
        "basis_round_id": maybe_text(draft.get("basis_round_id")),
        "generated_at_utc": utc_now_iso(),
        "status": "valid" if error_count == 0 else "invalid",
        "validation_scope": "structure-and-traceability-only",
        "does_not_decide": [
            "truth",
            "evidence sufficiency",
            "source ranking",
            "claim confidence",
        ],
        "issue_count": len(issues),
        "error_count": error_count,
        "warning_count": warning_count,
        "issues": issues,
        "draft_path": str(draft_file),
        "publish_allowed": error_count == 0,
    }
    write_json_file(output_file, validation)
    artifact_refs = [
        {"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"},
    ]
    return {
        "status": "completed" if error_count == 0 else "blocked",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "validation_id": validation_id,
            "validation_status": validation["status"],
            "error_count": error_count,
            "warning_count": warning_count,
            "output_path": str(output_file),
        },
        "receipt_id": "report-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, validation_id)[:20],
        "batch_id": "reportbatch-" + stable_hash(SKILL_NAME, run_id, round_id)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [validation_id],
        "warnings": [item for item in issues if item.get("severity") != "error"],
        "board_handoff": {
            "candidate_ids": [validation_id],
            "evidence_refs": artifact_refs,
            "gap_hints": [item["message"] for item in issues],
            "challenge_hints": [],
            "suggested_next_skills": ["publish-narrative-report"] if error_count == 0 else ["draft-narrative-report"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate a narrative report draft.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--draft-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = validate_narrative_report(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        draft_path=args.draft_path,
        output_path=args.output_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
