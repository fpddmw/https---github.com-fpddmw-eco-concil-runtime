from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from eco_council_runtime.objects.analysis import HELPER_DECISION_SOURCE_APPROVED_VIEW
from eco_council_runtime.objects.council import query_council_objects
from .support import (
    artifact_ref,
    dict_items,
    first_timestamp,
    helper_metadata,
    lineage_from_signals,
    list_items,
    maybe_text,
    query_signals,
    refs_from_signals,
    resolve_output_path,
    resolve_run_dir,
    safe_board_handoff,
    signal_source_distribution,
    stable_hash,
    text_terms,
    unique_texts,
    unique_values,
    utc_now_iso,
    write_json,
)


__all__ = (
    "approved_helper_input_payload",
    "explicit_approval_ref",
    "issue_terms_for_signal",
    "load_issue_hints_from_path",
    "load_json_file",
    "run_discover_discourse_issues",
    "run_export_research_issue_map",
    "run_materialize_research_issue_surface",
    "run_project_research_issue_views",
    "run_suggest_evidence_lanes",
    "signal_text",
    "unapproved_input_warning",
)


def run_discover_discourse_issues(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    output_path: str = "",
    limit: int = 300,
) -> dict[str, Any]:
    skill_name = "discover-discourse-issues"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"discourse_issue_discovery_{round_id}.json")
    public_signals, db_path = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="public", limit=limit)
    formal_signals, _ = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="formal", limit=limit)
    source_signals = [*public_signals, *formal_signals]
    term_counter = Counter()
    for signal in source_signals:
        term_counter.update(text_terms(" ".join([maybe_text(signal.get("title")), maybe_text(signal.get("body_text"))]), limit=20))
    terms = [term for term, _ in term_counter.most_common(8)]
    metadata = helper_metadata(
        skill_name=skill_name,
        rule_trace=["db-public-formal-signal-discourse-hints"],
        caveats=[
            "Discourse issue hints are not factual claims and do not define study scope.",
            "Mentioned scope metadata records text mentions only.",
        ],
    )
    hints: list[dict[str, Any]] = []
    if source_signals:
        for index, term in enumerate(terms or ["general-discourse"], start=1):
            members = [
                signal
                for signal in source_signals
                if term == "general-discourse" or term in " ".join([maybe_text(signal.get("title")), maybe_text(signal.get("body_text"))]).casefold()
            ]
            hint_id = "discourse-hint-" + stable_hash(run_id, round_id, term, index)[:12]
            snippets = [
                maybe_text(signal.get("body_text") or signal.get("title"))[:220]
                for signal in members[:5]
            ]
            hints.append(
                {
                    "hint_id": hint_id,
                    "run_id": run_id,
                    "round_id": round_id,
                    "hint_label": term,
                    "hint_kind": "public-discourse-issue-hint",
                    "member_signal_ids": [maybe_text(signal.get("signal_id")) for signal in members],
                    "text_evidence_snippets": snippets,
                    "source_distribution": signal_source_distribution(members),
                    "taxonomy_labels": [],
                    "mentioned_scope_metadata": {
                        "mentioned_places": [],
                        "mentioned_time_refs": unique_texts([first_timestamp(signal) for signal in members])[:8],
                        "mentioned_metrics": unique_texts([maybe_text(signal.get("metric")) for signal in members if maybe_text(signal.get("metric"))]),
                        "mentioned_policy_objects": [],
                        "mentioned_actors": unique_texts([maybe_text(signal.get("author_name")) for signal in members if maybe_text(signal.get("author_name"))])[:8],
                    },
                    "coverage_caveats": [
                        "Grouping confidence describes reversible text grouping only, not truth, importance, or representativeness."
                    ],
                    "evidence_refs": unique_values([ref for signal in members for ref in list_items(signal.get("evidence_refs"))]),
                    "lineage": [maybe_text(signal.get("signal_id")) for signal in members],
                    "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"], "db_path": db_path},
                }
            )
    payload = {
        "schema_version": "optional-analysis-discourse-issue-discovery-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "helper_governance": metadata,
        "discourse_issue_hints": hints,
        "observed_inputs": {"db_path": db_path, "public_signal_count": len(public_signals), "formal_signal_count": len(formal_signals)},
        "warnings": [] if hints else [{"code": "no-discourse-signals", "message": "No public or formal discourse signals were available."}],
    }
    write_json(output_file, payload)
    return {
        "status": "completed",
        "summary": {
            "skill": skill_name,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "hint_count": len(hints),
            "decision_source": metadata["decision_source"],
            "rule_id": metadata["rule_id"],
        },
        "receipt_id": "discourse-receipt-" + stable_hash(skill_name, run_id, round_id, output_file)[:20],
        "batch_id": "discourse-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.discourse_issue_hints")],
        "canonical_ids": [maybe_text(hint.get("hint_id")) for hint in hints],
        "warnings": payload["warnings"],
        "discourse_issue_hints": hints,
        "board_handoff": safe_board_handoff(
            artifact_path=output_file,
            locator="$.discourse_issue_hints",
            candidate_ids=[maybe_text(hint.get("hint_id")) for hint in hints],
            gap_hints=[] if hints else ["No discourse issue hints were available."],
        ),
    }


def load_json_file(path_text: str, default: Any) -> Any:
    text = maybe_text(path_text)
    if not text:
        return default
    path = Path(text).expanduser()
    if not path.exists():
        return default
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return default
    return payload


def explicit_approval_ref(value: Any) -> str:
    text = maybe_text(value)
    if not text or text.startswith("required:"):
        return ""
    return text


def approved_helper_input_payload(
    payload: Any,
    *,
    allowed_skills: set[str] | None = None,
) -> tuple[bool, str]:
    if not isinstance(payload, dict):
        return False, "input-artifact-is-not-object"
    metadata = dict_items(payload.get("helper_governance"))
    source_skill = maybe_text(metadata.get("skill") or payload.get("skill"))
    if allowed_skills is not None and source_skill not in allowed_skills:
        return False, "input-artifact-skill-not-allowed"
    if maybe_text(metadata.get("decision_source")) != HELPER_DECISION_SOURCE_APPROVED_VIEW:
        return False, "input-artifact-missing-approved-helper-view"
    if not maybe_text(metadata.get("rule_id")).startswith("HEUR-"):
        return False, "input-artifact-missing-rule-id"
    helper_status = maybe_text(metadata.get("helper_status"))
    audit_status = maybe_text(metadata.get("audit_status"))
    if "approval" not in helper_status and "approval" not in audit_status:
        return False, "input-artifact-missing-approval-gate-metadata"
    return True, ""


def unapproved_input_warning(input_path: str, reason: str) -> dict[str, str]:
    return {
        "code": "unapproved-input-artifact",
        "message": (
            f"Ignored input artifact {maybe_text(input_path) or '<empty>'}: "
            f"{reason}. Use DB-backed signals or an approved optional-analysis helper artifact."
        ),
    }


def run_suggest_evidence_lanes(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    input_path: str = "",
    output_path: str = "",
) -> dict[str, Any]:
    skill_name = "suggest-evidence-lanes"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"evidence_lane_suggestions_{round_id}.json")
    hints, input_warnings = load_issue_hints_from_path(
        input_path,
        allowed_skills={"discover-discourse-issues", "materialize-research-issue-surface"},
    )
    if not hints:
        findings = query_council_objects(run_dir_path, object_kind="finding", run_id=run_id, round_id=round_id, limit=100).get("objects", [])
        hints = [
            {
                "hint_id": maybe_text(item.get("finding_id")),
                "hint_label": maybe_text(item.get("title") or item.get("summary")),
                "text_evidence_snippets": [maybe_text(item.get("summary"))],
                "evidence_refs": list_items(item.get("evidence_refs")),
                "lineage": [maybe_text(item.get("finding_id"))],
            }
            for item in list_items(findings)
        ]
    metadata = helper_metadata(
        skill_name=skill_name,
        rule_trace=["advisory-evidence-lane-keyword-cues"],
        caveats=[
            "Lane suggestions are advisory tags only and cannot drive workflow, source queue, or phase transitions.",
            "This helper does not assign owners or readiness posture.",
        ],
    )
    suggestions: list[dict[str, Any]] = []
    for index, hint in enumerate(hints, start=1):
        text = " ".join([maybe_text(hint.get("hint_label")), " ".join(maybe_text(item) for item in list_items(hint.get("text_evidence_snippets")))]).casefold()
        lanes: list[str] = []
        if any(token in text for token in ("air", "water", "smoke", "flood", "soil", "river", "emission", "pollution")):
            lanes.append("environmental-evidence")
        if any(token in text for token in ("permit", "rule", "agency", "docket", "eia", "regulation")):
            lanes.append("formal-record")
        if any(token in text for token in ("community", "resident", "stakeholder", "concern", "public")):
            lanes.append("public-discourse")
        if not lanes:
            lanes.append("general-policy-research")
        suggestion_id = "lane-suggestion-" + stable_hash(run_id, round_id, maybe_text(hint.get("hint_id")), index)[:12]
        suggestions.append(
            {
                "suggestion_id": suggestion_id,
                "source_hint_id": maybe_text(hint.get("hint_id")),
                "advisory_lanes": unique_texts(lanes),
                "review_status": "human-review-only",
                "disabled_workflow_controls": ["owner-assignment", "queue-driver", "phase-transition"],
                "evidence_refs": list_items(hint.get("evidence_refs")),
                "lineage": unique_texts([maybe_text(hint.get("hint_id")), *list_items(hint.get("lineage"))]),
                "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"]},
            }
        )
    payload = {
        "schema_version": "optional-analysis-evidence-lane-suggestions-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "helper_governance": metadata,
        "suggestions": suggestions,
        "warnings": input_warnings + ([] if suggestions else [{"code": "no-approved-inputs", "message": "No discovery hints or finding records were available."}]),
    }
    write_json(output_file, payload)
    return {
        "status": "completed",
        "summary": {"skill": skill_name, "run_id": run_id, "round_id": round_id, "output_path": str(output_file), "suggestion_count": len(suggestions), "decision_source": metadata["decision_source"], "rule_id": metadata["rule_id"]},
        "receipt_id": "lane-receipt-" + stable_hash(skill_name, run_id, round_id, output_file)[:20],
        "batch_id": "lane-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.suggestions")],
        "canonical_ids": [maybe_text(item.get("suggestion_id")) for item in suggestions],
        "warnings": payload["warnings"],
        "suggestions": suggestions,
        "board_handoff": safe_board_handoff(artifact_path=output_file, locator="$.suggestions", candidate_ids=[maybe_text(item.get("suggestion_id")) for item in suggestions]),
    }


def signal_text(signal: dict[str, Any]) -> str:
    metadata = dict_items(signal.get("metadata"))
    raw = dict_items(signal.get("raw"))
    return " ".join(
        maybe_text(value)
        for value in (
            signal.get("title"),
            signal.get("body_text"),
            signal.get("author_name"),
            signal.get("channel_name"),
            metadata.get("docket_id"),
            metadata.get("agency_id"),
            metadata.get("submitter_name"),
            " ".join(unique_texts(list_items(metadata.get("issue_labels")))),
            " ".join(unique_texts(list_items(metadata.get("issue_terms")))),
            " ".join(unique_texts(list_items(metadata.get("concern_facets")))),
            " ".join(unique_texts(list_items(metadata.get("evidence_citation_types")))),
            maybe_text(raw.get("comment")),
            maybe_text(raw.get("text")),
        )
        if maybe_text(value)
    )


def issue_terms_for_signal(signal: dict[str, Any]) -> list[str]:
    metadata = dict_items(signal.get("metadata"))
    values: list[Any] = []
    values.extend(list_items(metadata.get("issue_labels")))
    values.extend(list_items(metadata.get("issue_terms")))
    values.extend(text_terms(signal_text(signal), limit=8))
    return unique_texts(values)[:8]


def load_issue_hints_from_path(
    input_path: str,
    *,
    allowed_skills: set[str] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    if not maybe_text(input_path):
        return [], []
    payload = load_json_file(input_path, None)
    if not isinstance(payload, dict):
        return [], [unapproved_input_warning(input_path, "input-artifact-missing-or-invalid")]
    approved, reason = approved_helper_input_payload(payload, allowed_skills=allowed_skills)
    if not approved:
        return [], [unapproved_input_warning(input_path, reason)]
    for field_name in (
        "research_issues",
        "issue_views",
        "discourse_issue_hints",
        "suggestions",
    ):
        items = list_items(payload.get(field_name))
        if items:
            return [dict_items(item) for item in items], []
    return [], []


def run_materialize_research_issue_surface(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    input_path: str = "",
    output_path: str = "",
    limit: int = 400,
) -> dict[str, Any]:
    skill_name = "materialize-research-issue-surface"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"research_issue_surface_{round_id}.json")
    loaded_hints, input_warnings = load_issue_hints_from_path(
        input_path,
        allowed_skills={"discover-discourse-issues", "suggest-evidence-lanes"},
    )
    public_signals, db_path = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="public", limit=limit)
    formal_signals, _ = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="formal", limit=limit)
    signals = [*public_signals, *formal_signals]
    metadata = helper_metadata(
        skill_name=skill_name,
        rule_trace=["db-discourse-signal-issue-surface"],
        caveats=[
            "Research issue records are reversible issue-surface cues, not factual conclusions.",
            "Report use requires moderator-approved DB basis objects.",
        ],
    )
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for signal in signals:
        terms = issue_terms_for_signal(signal)
        bucket = terms[0] if terms else "general-policy-issue"
        buckets[bucket].append(signal)
    if loaded_hints and not buckets:
        for hint in loaded_hints:
            label = maybe_text(hint.get("issue_label") or hint.get("hint_label") or hint.get("source_hint_id") or "input-policy-issue")
            buckets[label].append(
                {
                    "signal_id": maybe_text(hint.get("issue_id") or hint.get("hint_id") or hint.get("suggestion_id")),
                    "title": label,
                    "body_text": " ".join(maybe_text(item) for item in list_items(hint.get("text_evidence_snippets"))),
                    "source_skill": maybe_text(hint.get("skill")),
                    "evidence_refs": list_items(hint.get("evidence_refs")),
                    "metadata": {},
                    "raw": {},
                }
            )
    issues: list[dict[str, Any]] = []
    for index, (label, members) in enumerate(sorted(buckets.items()), start=1):
        issue_id = "research-issue-" + stable_hash(run_id, round_id, label, index)[:12]
        member_terms = unique_texts([term for signal in members for term in issue_terms_for_signal(signal)])
        issue = {
            "issue_id": issue_id,
            "run_id": run_id,
            "round_id": round_id,
            "issue_label": maybe_text(label),
            "issue_terms": member_terms[:12],
            "source_signal_ids": lineage_from_signals(members),
            "source_distribution": signal_source_distribution(members),
            "issue_surface_status": "candidate-for-human-review",
            "report_usage": "appendix-or-audit-only-until-db-basis-cites-it",
            "evidence_refs": refs_from_signals(members),
            "lineage": lineage_from_signals(members),
            "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"], "db_path": db_path},
            "helper_governance": metadata,
        }
        issues.append(issue)
    payload = {
        "schema_version": "optional-analysis-research-issue-surface-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "helper_governance": metadata,
        "research_issues": issues,
        "observed_inputs": {
            "db_path": db_path,
            "public_signal_count": len(public_signals),
            "formal_signal_count": len(formal_signals),
            "input_hint_count": len(loaded_hints),
            "input_artifact_warning_count": len(input_warnings),
        },
        "warnings": input_warnings + ([] if issues else [{"code": "no-issue-surface-inputs", "message": "No DB discourse signals or approved input hints were available."}]),
    }
    write_json(output_file, payload)
    return {
        "status": "completed",
        "summary": {"skill": skill_name, "run_id": run_id, "round_id": round_id, "output_path": str(output_file), "issue_count": len(issues), "decision_source": metadata["decision_source"], "rule_id": metadata["rule_id"]},
        "receipt_id": "issue-surface-receipt-" + stable_hash(skill_name, run_id, round_id, output_file)[:20],
        "batch_id": "issue-surface-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.research_issues")],
        "canonical_ids": [maybe_text(item.get("issue_id")) for item in issues],
        "warnings": payload["warnings"],
        "research_issues": issues,
        "board_handoff": safe_board_handoff(artifact_path=output_file, locator="$.research_issues", candidate_ids=[maybe_text(item.get("issue_id")) for item in issues]),
    }


def run_project_research_issue_views(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    input_path: str = "",
    output_path: str = "",
    limit: int = 400,
) -> dict[str, Any]:
    skill_name = "project-research-issue-views"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"research_issue_views_{round_id}.json")
    issues, input_warnings = load_issue_hints_from_path(
        input_path,
        allowed_skills={"materialize-research-issue-surface", "discover-discourse-issues"},
    )
    public_signals, db_path = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="public", limit=limit)
    formal_signals, _ = query_signals(run_dir_path, run_id=run_id, round_id=round_id, plane="formal", limit=limit)
    signals = [*public_signals, *formal_signals]
    if not issues and signals:
        issues = [
            {
                "issue_id": "research-issue-" + stable_hash(run_id, round_id, "all-discourse")[:12],
                "issue_label": "round-discourse-surface",
                "issue_terms": unique_texts([term for signal in signals for term in issue_terms_for_signal(signal)])[:12],
                "source_signal_ids": lineage_from_signals(signals),
                "evidence_refs": refs_from_signals(signals),
                "lineage": lineage_from_signals(signals),
            }
        ]
    metadata = helper_metadata(
        skill_name=skill_name,
        rule_trace=["db-discourse-typed-projection-cues"],
        caveats=[
            "Typed projections are candidate cues only and must remain auditable.",
            "This helper does not write report prose or research conclusions.",
        ],
    )
    views: list[dict[str, Any]] = []
    for index, issue in enumerate(issues, start=1):
        issue_terms = unique_texts(list_items(issue.get("issue_terms")))
        issue_ids = set(unique_texts(list_items(issue.get("source_signal_ids"))))
        members = [signal for signal in signals if not issue_ids or maybe_text(signal.get("signal_id")) in issue_ids]
        if not members and signals:
            members = signals
        metadata_items = [dict_items(signal.get("metadata")) for signal in members]
        actor_cues = unique_texts(
            [signal.get("author_name") for signal in members]
            + [metadata.get("submitter_name") for metadata in metadata_items]
        )[:20]
        concern_cues = unique_texts(
            [
                cue
                for metadata in metadata_items
                for cue in list_items(metadata.get("concern_facets"))
            ]
            + issue_terms
        )[:20]
        citation_cues = unique_texts(
            [
                cue
                for metadata in metadata_items
                for cue in list_items(metadata.get("evidence_citation_types"))
            ]
        )[:20]
        stance_cues = unique_texts([metadata.get("stance_hint") for metadata in metadata_items])[:20]
        view_id = "issue-view-" + stable_hash(run_id, round_id, maybe_text(issue.get("issue_id")), index)[:12]
        views.append(
            {
                "view_id": view_id,
                "issue_id": maybe_text(issue.get("issue_id")),
                "issue_label": maybe_text(issue.get("issue_label")),
                "typed_cues": {
                    "actor_cues": actor_cues,
                    "concern_cues": concern_cues,
                    "citation_cues": citation_cues,
                    "stance_cues": stance_cues,
                },
                "projection_status": "candidate-for-human-review",
                "evidence_refs": refs_from_signals(members) or list_items(issue.get("evidence_refs")),
                "lineage": unique_texts([maybe_text(issue.get("issue_id")), *lineage_from_signals(members), *list_items(issue.get("lineage"))]),
                "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"], "db_path": db_path},
                "helper_governance": metadata,
            }
        )
    payload = {
        "schema_version": "optional-analysis-research-issue-views-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "helper_governance": metadata,
        "issue_views": views,
        "observed_inputs": {"db_path": db_path, "issue_count": len(issues), "signal_count": len(signals), "input_artifact_warning_count": len(input_warnings)},
        "warnings": input_warnings + ([] if views else [{"code": "no-issue-view-inputs", "message": "No issue surface or DB discourse signals were available."}]),
    }
    write_json(output_file, payload)
    return {
        "status": "completed",
        "summary": {"skill": skill_name, "run_id": run_id, "round_id": round_id, "output_path": str(output_file), "view_count": len(views), "decision_source": metadata["decision_source"], "rule_id": metadata["rule_id"]},
        "receipt_id": "issue-view-receipt-" + stable_hash(skill_name, run_id, round_id, output_file)[:20],
        "batch_id": "issue-view-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.issue_views")],
        "canonical_ids": [maybe_text(item.get("view_id")) for item in views],
        "warnings": payload["warnings"],
        "issue_views": views,
        "board_handoff": safe_board_handoff(artifact_path=output_file, locator="$.issue_views", candidate_ids=[maybe_text(item.get("view_id")) for item in views]),
    }


def run_export_research_issue_map(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    issue_surface_path: str = "",
    issue_views_path: str = "",
    output_path: str = "",
) -> dict[str, Any]:
    skill_name = "export-research-issue-map"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"research_issue_map_{round_id}.json")
    warnings: list[dict[str, str]] = []
    issues, issue_warnings = load_issue_hints_from_path(
        issue_surface_path,
        allowed_skills={"materialize-research-issue-surface"},
    )
    views, view_warnings = load_issue_hints_from_path(
        issue_views_path,
        allowed_skills={"project-research-issue-views"},
    )
    warnings.extend(issue_warnings)
    warnings.extend(view_warnings)
    if not issues:
        default_surface = run_dir_path / "analytics" / f"research_issue_surface_{round_id}.json"
        issues, issue_warnings = load_issue_hints_from_path(
            str(default_surface),
            allowed_skills={"materialize-research-issue-surface"},
        )
        warnings.extend(issue_warnings)
    if not views:
        default_views = run_dir_path / "analytics" / f"research_issue_views_{round_id}.json"
        views, view_warnings = load_issue_hints_from_path(
            str(default_views),
            allowed_skills={"project-research-issue-views"},
        )
        warnings.extend(view_warnings)
    metadata = helper_metadata(
        skill_name=skill_name,
        rule_trace=["research-issue-map-export"],
        caveats=[
            "The issue map is a navigation export, not a conclusion graph.",
            "Edges are traceability cues only and do not imply causal relationships.",
        ],
    )
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    for issue in issues:
        issue_id = maybe_text(issue.get("issue_id") or issue.get("hint_id"))
        if not issue_id:
            continue
        nodes.append(
            {
                "node_id": issue_id,
                "node_kind": "research-issue",
                "label": maybe_text(issue.get("issue_label") or issue.get("hint_label")),
                "evidence_refs": list_items(issue.get("evidence_refs")),
                "lineage": list_items(issue.get("lineage")),
                "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"]},
            }
        )
    for view in views:
        view_id = maybe_text(view.get("view_id"))
        issue_id = maybe_text(view.get("issue_id"))
        if not view_id:
            continue
        nodes.append(
            {
                "node_id": view_id,
                "node_kind": "issue-view",
                "label": maybe_text(view.get("issue_label")),
                "evidence_refs": list_items(view.get("evidence_refs")),
                "lineage": list_items(view.get("lineage")),
                "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"]},
            }
        )
        if issue_id:
            edges.append(
                {
                    "edge_id": "issue-map-edge-" + stable_hash(run_id, round_id, issue_id, view_id)[:12],
                    "from_node_id": issue_id,
                    "to_node_id": view_id,
                    "relationship_kind": "traceability-cue",
                    "evidence_refs": list_items(view.get("evidence_refs")),
                    "lineage": unique_texts([issue_id, view_id, *list_items(view.get("lineage"))]),
                    "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"]},
                }
            )
    issue_map = {
        "map_id": "research-issue-map-" + stable_hash(run_id, round_id, len(nodes), len(edges))[:12],
        "run_id": run_id,
        "round_id": round_id,
        "nodes": nodes,
        "edges": edges,
        "map_status": "navigation-export",
        "helper_governance": metadata,
    }
    payload = {
        "schema_version": "optional-analysis-research-issue-map-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "research_issue_map": issue_map,
        "warnings": warnings + ([] if nodes else [{"code": "no-map-inputs", "message": "No issue surface or issue views were available."}]),
    }
    write_json(output_file, payload)
    return {
        "status": "completed",
        "summary": {"skill": skill_name, "run_id": run_id, "round_id": round_id, "output_path": str(output_file), "node_count": len(nodes), "edge_count": len(edges), "decision_source": metadata["decision_source"], "rule_id": metadata["rule_id"]},
        "receipt_id": "issue-map-receipt-" + stable_hash(skill_name, run_id, round_id, output_file)[:20],
        "batch_id": "issue-map-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.research_issue_map")],
        "canonical_ids": [issue_map["map_id"]] if nodes else [],
        "warnings": payload["warnings"],
        "research_issue_map": issue_map,
        "board_handoff": safe_board_handoff(artifact_path=output_file, locator="$.research_issue_map", candidate_ids=[issue_map["map_id"]] if nodes else []),
    }
