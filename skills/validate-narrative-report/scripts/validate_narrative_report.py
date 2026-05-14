#!/usr/bin/env python3
"""Validate narrative report draft structure and traceability."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
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
PUBLIC_DISCOURSE_BASIS_MARKERS = (
    "public_discourse_sample_summary",
    "public-discourse-sample-summary",
    "summarize-public-discourse-sample",
    "public_discourse_annotation_aggregation",
    "public-discourse-annotation-aggregation",
    "aggregate-public-discourse-annotations",
    "classify-public-discourse-affect",
)
PUBLIC_OPINION_UPGRADE_PHRASES = (
    "overall public opinion",
    "general public opinion",
    "representative public opinion",
    "representative public sentiment",
    "platform-wide sentiment",
    "affected population opinion",
    "affected-population opinion",
    "population opinion",
    "public sentiment estimate",
    "overall public sentiment",
    "\u603b\u4f53\u6c11\u610f",
    "\u6574\u4f53\u6c11\u610f",
    "\u4ee3\u8868\u6027\u6c11\u610f",
    "\u4ee3\u8868\u6027\u516c\u4f17\u60c5\u7eea",
    "\u5e73\u53f0\u6574\u4f53\u60c5\u7eea",
    "\u53d7\u5f71\u54cd\u4eba\u7fa4\u6574\u4f53\u89c2\u70b9",
    "\u53d7\u5f71\u54cd\u4eba\u7fa4\u603b\u4f53\u89c2\u70b9",
)
GDELT_TONE_PUBLIC_SENTIMENT_PHRASES = (
    "gdelt tone proves public sentiment",
    "gdelt tone shows public sentiment",
    "gdelt v2tone proves public sentiment",
    "gdelt v2tone shows public sentiment",
    "gdelt tone represents public sentiment",
    "gdelt tone represents public emotion",
    "gdelt tone is public sentiment",
    "gdelt tone \u662f\u516c\u4f17\u60c5\u7eea",
    "gdelt tone \u4ee3\u8868\u516c\u4f17\u60c5\u7eea",
    "gdelt tone \u8bc1\u660e\u516c\u4f17\u60c5\u7eea",
    "gdelt v2tone \u8bc1\u660e\u516c\u4f17\u60c5\u7eea",
)
SOURCE_NARRATIVE_ATTRIBUTION_PHRASES = (
    "source narrative proves physical source attribution",
    "source narrative establishes physical source attribution",
    "public source narrative proves physical source",
    "public narrative proves physical source attribution",
    "source narrative proves origin",
    "\u6765\u6e90\u53d9\u4e8b\u8bc1\u660e\u7269\u7406\u6765\u6e90",
    "\u6765\u6e90\u53d9\u4e8b\u8bc1\u660e\u6765\u6e90\u5f52\u56e0",
    "\u516c\u5171\u6765\u6e90\u53d9\u4e8b\u8bc1\u660e\u7269\u7406\u5f52\u56e0",
    "\u516c\u5171\u53d9\u4e8b\u8bc1\u660e\u5177\u4f53\u6765\u6e90",
)
ENVIRONMENT_ATTRIBUTION_PHRASES = (
    "caused by",
    "causal attribution",
    "source attribution",
    "transport attribution",
    "specific source",
    "specific origin",
    "specific fire",
    "proved the source",
    "proves the source",
    "\u6765\u6e90\u5f52\u56e0",
    "\u56e0\u679c\u5f52\u56e0",
    "\u8f93\u9001\u5f52\u56e0",
    "\u5177\u4f53\u6e90\u5934",
    "\u5177\u4f53\u6765\u6e90",
    "\u5177\u4f53\u706b\u573a",
    "\u8bc1\u660e\u6765\u6e90",
)
ATTRIBUTION_MODEL_MARKERS = (
    "trajectory",
    "back trajectory",
    "plume",
    "chemistry",
    "chemical",
    "attribution model",
    "smoke model",
    "\u53cd\u5411\u8f68\u8ff9",
    "\u8f68\u8ff9",
    "\u70df\u7fbd",
    "\u5316\u5b66",
    "\u5f52\u56e0\u6a21\u578b",
)
ACQUISITION_ATTEMPT_TERMS = (
    "zero-signal",
    "zero signal",
    "receipt-only",
    "failed acquisition",
    "blocked acquisition",
    "executed-without-normalized-refs",
    "no normalized refs",
)
ACTIONABLE_PATH_TERMS = (
    "actionable path",
    "actionable route",
    "non-continuation rationale",
    "continue investigation",
    "continuation round",
    "no-actionable-path",
    "\u53ef\u884c\u52a8\u8c03\u67e5\u8def\u5f84",
    "\u7ee7\u7eed\u8c03\u67e5",
    "\u4e0d\u7ee7\u7eed\u8c03\u67e5\u7406\u7531",
)
NEGATION_CUES = (
    "not ",
    "not a ",
    "not as ",
    "not be ",
    "must not ",
    "do not ",
    "cannot ",
    "can't ",
    "without ",
    "does not ",
    "should not ",
    "\u4e0d\u5f97",
    "\u4e0d\u80fd",
    "\u4e0d\u5e94",
    "\u4e0d\u662f",
    "\u5e76\u975e",
    "\u4e0d\u53ef",
    "\u672a\u80fd",
    "\u7f3a\u5c11",
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


def load_json_file_if_exists(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return load_json_file(path)


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def issue(code: str, message: str, severity: str = "warning") -> dict[str, str]:
    return {"code": code, "severity": severity, "message": message}


def strings_from(value: Any) -> list[str]:
    results: list[str] = []
    if isinstance(value, dict):
        for child in value.values():
            results.extend(strings_from(child))
    elif isinstance(value, list):
        for child in value:
            results.extend(strings_from(child))
    else:
        text = maybe_text(value)
        if text:
            results.append(text)
    return results


def report_prose_text(draft: dict[str, Any]) -> str:
    parts: list[Any] = [draft.get("title")]
    boundary = draft.get("claim_boundary") if isinstance(draft.get("claim_boundary"), dict) else {}
    parts.append(boundary.get("summary"))
    sections = draft.get("sections") if isinstance(draft.get("sections"), list) else []
    for section in sections:
        if not isinstance(section, dict):
            continue
        parts.append(section.get("title"))
        paragraphs = section.get("paragraphs") if isinstance(section.get("paragraphs"), list) else []
        parts.extend(paragraphs)
    return "\n".join(unique_texts(parts))


def normalized_text(text: str) -> str:
    return maybe_text(text).casefold()


def phrase_is_negated(text: str, start: int) -> bool:
    prefix = text[max(0, start - 64) : start]
    return any(cue in prefix for cue in NEGATION_CUES)


def contains_unnegated_phrase(text: str, phrases: tuple[str, ...]) -> bool:
    lowered = normalized_text(text)
    for phrase in phrases:
        lowered_phrase = phrase.casefold()
        search_from = 0
        while True:
            index = lowered.find(lowered_phrase, search_from)
            if index < 0:
                break
            if not phrase_is_negated(lowered, index):
                return True
            search_from = index + len(lowered_phrase)
    return False


def contains_any_phrase(text: str, phrases: tuple[str, ...]) -> bool:
    lowered = normalized_text(text)
    return any(phrase.casefold() in lowered for phrase in phrases)


def section_by_id(draft: dict[str, Any], section_id: str) -> dict[str, Any]:
    sections = draft.get("sections") if isinstance(draft.get("sections"), list) else []
    for section in sections:
        if isinstance(section, dict) and maybe_text(section.get("section_id")) == section_id:
            return section
    return {}


def all_evidence_refs(draft: dict[str, Any]) -> list[str]:
    refs = list(draft.get("evidence_refs")) if isinstance(draft.get("evidence_refs"), list) else []
    sections = draft.get("sections") if isinstance(draft.get("sections"), list) else []
    for section in sections:
        if isinstance(section, dict) and isinstance(section.get("evidence_refs"), list):
            refs.extend(section["evidence_refs"])
    return unique_texts(refs)


def council_object_counts(draft: dict[str, Any]) -> dict[str, int]:
    source_material = draft.get("source_material") if isinstance(draft.get("source_material"), dict) else {}
    counts = (
        source_material.get("council_object_counts")
        if isinstance(source_material.get("council_object_counts"), dict)
        else {}
    )
    normalized: dict[str, int] = {}
    for key, value in counts.items():
        if isinstance(key, str):
            try:
                normalized[key] = int(value)
            except (TypeError, ValueError):
                normalized[key] = 0
    return normalized


def has_public_discourse_basis(draft: dict[str, Any], text: str) -> bool:
    source_material = draft.get("source_material") if isinstance(draft.get("source_material"), dict) else {}
    public_summary = (
        source_material.get("public_discourse_summary")
        if isinstance(source_material.get("public_discourse_summary"), dict)
        else {}
    )
    if any(maybe_text(public_summary.get(key)) for key in ("path", "summary_id", "status")):
        return True
    refs_text = "\n".join(all_evidence_refs(draft))
    return contains_any_phrase("\n".join([text, refs_text]), PUBLIC_DISCOURSE_BASIS_MARKERS)


def public_discourse_summary_contract_issues(
    draft: dict[str, Any],
    *,
    run_dir: Path,
) -> list[dict[str, str]]:
    source_material = draft.get("source_material") if isinstance(draft.get("source_material"), dict) else {}
    public_summary_meta = (
        source_material.get("public_discourse_summary")
        if isinstance(source_material.get("public_discourse_summary"), dict)
        else {}
    )
    public_section = section_by_id(draft, "public-discourse-deepening")
    if not public_section and not any(maybe_text(public_summary_meta.get(key)) for key in ("path", "summary_id", "status")):
        return []

    issues: list[dict[str, str]] = []
    summary_path_text = maybe_text(public_summary_meta.get("path"))
    if not summary_path_text:
        issues.append(
            issue(
                "public-summary-path-missing",
                "Public discourse addendum metadata should include the helper artifact path.",
                "warning",
            )
        )
        return issues

    summary_path = resolve_path(run_dir, summary_path_text, summary_path_text)
    summary_payload = load_json_file_if_exists(summary_path)
    if not summary_payload:
        issues.append(
            issue(
                "public-summary-artifact-missing",
                "Public discourse addendum metadata points to a helper artifact that was not found.",
                "warning",
            )
        )
        return issues

    if maybe_text(summary_payload.get("schema_version")) != "optional-analysis-public-discourse-sample-summary-v1":
        issues.append(
            issue(
                "public-summary-unexpected-schema",
                "Public discourse summary should use optional-analysis-public-discourse-sample-summary-v1.",
                "warning",
            )
        )
    if maybe_text(summary_payload.get("skill")) != "summarize-public-discourse-sample":
        issues.append(
            issue(
                "public-summary-unexpected-skill",
                "Public discourse summary should come from summarize-public-discourse-sample or equivalent approved helper basis.",
                "warning",
            )
        )
    required_fields = (
        "sample_definition",
        "source_family_counts",
        "discourse_lane_counts",
        "warnings",
        "evidence_refs",
        "distribution_use_policy",
    )
    for field_name in required_fields:
        value = summary_payload.get(field_name)
        if field_name in {"sample_definition", "distribution_use_policy"}:
            present = isinstance(value, dict) and bool(value)
        else:
            present = isinstance(value, list)
        if not present:
            issues.append(
                issue(
                    "public-summary-contract-incomplete",
                    f"Public discourse summary is missing or has an invalid `{field_name}` field.",
                    "warning",
                )
            )
    has_distribution = any(
        isinstance(summary_payload.get(field_name), list) and bool(summary_payload.get(field_name))
        for field_name in (
            "issue_distribution",
            "social_affect_distribution",
            "source_narrative_distribution",
            "actor_responsibility_distribution",
            "action_orientation_distribution",
        )
    )
    if not has_distribution:
        issues.append(
            issue(
                "public-summary-no-label-distribution",
                "Public discourse summary carries no label distributions; report prose should stay at visibility/source-family boundary.",
                "warning",
            )
        )
    policy = summary_payload.get("distribution_use_policy") if isinstance(summary_payload.get("distribution_use_policy"), dict) else {}
    expected_policy = {
        "label_sets_are_non_exclusive": True,
        "sample_fractions_are_sample_local": True,
        "do_not_sum_to_population_opinion": True,
        "requires_council_uptake_before_reporting": True,
    }
    for key, expected in expected_policy.items():
        if policy.get(key) is not expected:
            issues.append(
                issue(
                    "public-summary-policy-boundary-missing",
                    f"Public discourse summary distribution_use_policy should set `{key}` to true.",
                    "warning",
                )
            )
    if maybe_text(policy.get("gdelt_tone_boundary")) not in {"media_or_document_tone_not_public_sentiment", ""}:
        issues.append(
            issue(
                "public-summary-gdelt-boundary-unexpected",
                "Public discourse summary has an unexpected GDELT tone boundary marker.",
                "warning",
            )
        )
    if maybe_text(policy.get("source_narrative_boundary")) not in {"public_source_narrative_cue_not_physical_source_attribution", ""}:
        issues.append(
            issue(
                "public-summary-source-narrative-boundary-unexpected",
                "Public discourse summary has an unexpected source narrative boundary marker.",
                "warning",
            )
        )
    return issues


def has_optional_analysis_carrier(draft: dict[str, Any], helper_id: str) -> bool:
    source_material = draft.get("source_material") if isinstance(draft.get("source_material"), dict) else {}
    reporting_artifacts = (
        source_material.get("reporting_artifacts")
        if isinstance(source_material.get("reporting_artifacts"), list)
        else []
    )
    reporting_artifact_text = "\n".join(strings_from(reporting_artifacts))
    if helper_id and helper_id.casefold() in reporting_artifact_text.casefold():
        return True
    if contains_any_phrase(reporting_artifact_text, PUBLIC_DISCOURSE_BASIS_MARKERS):
        return True
    return any(
        isinstance(row, dict) and maybe_text(row.get("kind")) == "report-basis-freeze"
        for row in reporting_artifacts
    )


def sample_distribution_language_present(text: str) -> bool:
    lowered = normalized_text(text)
    has_sample_language = any(
        marker in lowered
        for marker in (
            "sample-local",
            "sample local",
            "sample fraction",
            "sample_fraction",
            "sample distribution",
            "sample-level",
            "\u6837\u672c\u5185",
            "\u6837\u672c\u6bd4\u4f8b",
            "\u6837\u672c\u5206\u5e03",
        )
    )
    has_ratio = bool(re.search(r"\b\d+(?:\.\d+)?\s*%", lowered)) or "\u51fa\u73b0\u7387" in lowered
    return has_sample_language and has_ratio


def validate_claim_boundary_semantics(draft: dict[str, Any]) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    text = report_prose_text(draft)
    if not text:
        return issues

    if contains_unnegated_phrase(text, PUBLIC_OPINION_UPGRADE_PHRASES):
        issues.append(
            issue(
                "unsupported-public-opinion-claim",
                (
                    "Report text appears to make a representative or platform-wide "
                    "public-opinion claim. Keep public discourse language sample-local "
                    "unless the mission records a representative sampling design."
                ),
                "error",
            )
        )

    if sample_distribution_language_present(text) and not has_public_discourse_basis(draft, text):
        issues.append(
            issue(
                "sample-distribution-without-public-discourse-basis",
                (
                    "Sample-local percentages or label distributions require a public "
                    "discourse summary, annotation aggregation, or equivalent DB-backed helper basis."
                ),
                "error",
            )
        )

    if contains_unnegated_phrase(text, GDELT_TONE_PUBLIC_SENTIMENT_PHRASES):
        issues.append(
            issue(
                "gdelt-tone-public-sentiment",
                "GDELT tone may describe media/document tone, not public sentiment or public emotion.",
                "error",
            )
        )

    if contains_unnegated_phrase(text, SOURCE_NARRATIVE_ATTRIBUTION_PHRASES):
        issues.append(
            issue(
                "source-narrative-as-physical-attribution",
                (
                    "Public source narratives must remain source-narrative cues and "
                    "must not substitute for physical source attribution."
                ),
                "error",
            )
        )

    if contains_unnegated_phrase(text, ENVIRONMENT_ATTRIBUTION_PHRASES):
        counts = council_object_counts(draft)
        has_environment_basis = any(counts.get(kind, 0) > 0 for kind in ("finding", "evidence-bundle"))
        has_challenger_review = counts.get("review-comment", 0) > 0 or counts.get("challenge", 0) > 0
        if not has_environment_basis:
            issues.append(
                issue(
                    "attribution-claim-needs-environment-basis",
                    (
                        "Environmental source, transport, causal, or origin claims "
                        "should cite environment findings, evidence bundles, hypotheses, "
                        "or equivalent DB-backed basis."
                    ),
                    "warning",
                )
            )
        if not has_challenger_review:
            issues.append(
                issue(
                    "attribution-claim-needs-challenger-visibility",
                    (
                        "Strong attribution language should keep challenger review, "
                        "alternative explanations, or explicit limitation handling visible."
                    ),
                    "warning",
                )
            )
        if not contains_any_phrase(text, ATTRIBUTION_MODEL_MARKERS):
            issues.append(
                issue(
                    "attribution-model-limitation-not-visible",
                    (
                        "If the report discusses source, transport, or causal attribution, "
                        "it should state whether trajectory, plume, chemistry, or comparable "
                        "attribution evidence is present or absent."
                    ),
                    "warning",
                )
            )

    public_section = section_by_id(draft, "public-discourse-deepening")
    source_material = draft.get("source_material") if isinstance(draft.get("source_material"), dict) else {}
    public_summary = (
        source_material.get("public_discourse_summary")
        if isinstance(source_material.get("public_discourse_summary"), dict)
        else {}
    )
    public_summary_id = maybe_text(public_summary.get("summary_id"))
    if public_section and public_summary and not has_optional_analysis_carrier(draft, public_summary_id):
        issues.append(
            issue(
                "optional-analysis-not-carried",
                (
                    "The public discourse addendum uses an advisory helper output. "
                    "Confirm it is carried by a finding, bundle, position, readiness, "
                    "synthesis, or report-basis object before treating it as report basis."
                ),
                "warning",
            )
        )

    if contains_any_phrase(text, ACQUISITION_ATTEMPT_TERMS) and not contains_any_phrase(text, ACTIONABLE_PATH_TERMS):
        issues.append(
            issue(
                "acquisition-attempt-without-actionable-path-rationale",
                (
                    "Failed, blocked, receipt-only, zero-signal, or no-normalized-ref "
                    "acquisition attempts should be paired with an explicit actionable-path "
                    "or non-continuation rationale before report closure."
                ),
                "warning",
            )
        )

    return issues


def validate_draft(draft: dict[str, Any], *, run_dir: Path | None = None) -> list[dict[str, str]]:
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
    issues.extend(validate_claim_boundary_semantics(draft))
    if run_dir is not None:
        issues.extend(public_discourse_summary_contract_issues(draft, run_dir=run_dir))
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
    issues = validate_draft(draft, run_dir=run_dir_path)
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
        "validation_scope": "structure-traceability-and-claim-boundary",
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
