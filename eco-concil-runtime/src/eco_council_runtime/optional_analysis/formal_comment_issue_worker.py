from __future__ import annotations

from typing import Any

from eco_council_runtime.formal_signal_semantics import (
    CONCERN_RULES,
    ISSUE_RULES,
    STANCE_RULES,
    FORMAL_PUBLIC_TAXONOMY_VERSION,
)

from .research_issues import signal_text
from .support import (
    artifact_ref,
    dict_items,
    helper_metadata,
    list_items,
    maybe_text,
    query_signals,
    resolve_output_path,
    resolve_run_dir,
    safe_board_handoff,
    stable_hash,
    unique_texts,
    utc_now_iso,
    write_json,
)


DEFAULT_ANNOTATION_BASIS_REF = "formal-comment-issue-worker-v1"
FORMAL_COMMENT_SOURCE_SKILLS = {
    "fetch-regulationsgov-comments",
    "fetch-regulationsgov-comment-detail",
    "fetch-regulationsgov-attachments",
}
FORMAL_COMMENT_TEXT_KINDS = {
    "comment",
    "comment-listing",
    "comment-detail",
    "attachment-text",
    "formal-signal",
}
FORMAL_LABEL_FAMILY_RULES: dict[str, dict[str, tuple[str, ...]]] = {
    "formal_issue_labels": {
        "health-benefit": (
            "health",
            "public health",
            "asthma",
            "respiratory",
            "mortality",
            "premature death",
            "pm2.5",
            "particulate",
            "benefit",
        ),
        "compliance-cost": (
            "compliance cost",
            "cost of compliance",
            "costly",
            "expensive",
            "retrofit",
            "control cost",
        ),
        "monitoring-implementation": (
            "monitoring",
            "implementation",
            "implement",
            "deadline",
            "timeline",
            "attainment",
            "permit",
            "enforceable",
        ),
        "environmental-justice": (
            "environmental justice",
            "frontline",
            "overburdened",
            "low-income",
            "equity",
            "disadvantaged",
        ),
        "scientific-basis": (
            "scientific basis",
            "science",
            "evidence",
            "study",
            "research",
            "epidemiological",
            "data",
            "model",
        ),
        "legal-authority": (
            "legal",
            "authority",
            "statutory",
            "clean air act",
            "jurisdiction",
            "arbitrary",
            "capricious",
        ),
        "economic-burden": (
            "economic burden",
            "burden",
            "jobs",
            "employment",
            "small business",
            "livelihood",
            "income",
        ),
        "industry-impact": (
            "industry",
            "manufacturer",
            "facility",
            "plant",
            "forestry",
            "business",
            "utility",
        ),
        "state-local-implementation": (
            "state implementation",
            "local implementation",
            "state",
            "local",
            "county",
            "municipal",
            "sip",
        ),
    },
    "formal_stance_hints": {
        "support": (
            "support",
            "approve",
            "strengthen",
            "beneficial",
            "protect",
            "needed",
            "necessary",
        ),
        "oppose": (
            "oppose",
            "object",
            "withdraw",
            "should not",
            "too stringent",
            "unacceptable",
            "reject",
        ),
        "mixed": (
            "mixed",
            "however",
            "while we support",
            "support some",
            "partial",
            "with reservations",
        ),
        "procedural-or-unclear": (
            "extend comment",
            "comment period",
            "hearing",
            "reopen",
            "clarify",
            "process",
            "procedural",
        ),
    },
    "formal_concern_facets": {
        "health": (
            "health",
            "asthma",
            "respiratory",
            "children",
            "mortality",
            "illness",
        ),
        "cost": ("cost", "expensive", "burden", "economic", "jobs", "small business"),
        "feasibility": (
            "feasible",
            "feasibility",
            "implement",
            "implementation",
            "timeline",
            "deadline",
            "available technology",
        ),
        "uncertainty": (
            "uncertain",
            "uncertainty",
            "unclear",
            "data gap",
            "insufficient data",
            "model",
        ),
        "equity": (
            "equity",
            "justice",
            "frontline",
            "low-income",
            "overburdened",
            "disadvantaged",
        ),
        "federalism": (
            "federalism",
            "state authority",
            "state implementation",
            "local authority",
            "state discretion",
        ),
    },
}
FORMAL_SEMANTICS_FALLBACKS = {
    "formal_issue_labels": ISSUE_RULES,
    "formal_concern_facets": CONCERN_RULES,
    "formal_stance_hints": STANCE_RULES,
}


def _matched_cues(text: str, cues: tuple[str, ...]) -> list[str]:
    folded = maybe_text(text).casefold()
    matches: list[str] = []
    for cue in cues:
        cue_text = maybe_text(cue)
        if cue_text and cue_text.casefold() in folded:
            matches.append(cue_text)
    return unique_texts(matches)


def _metadata_label_rows(signal: dict[str, Any]) -> list[dict[str, Any]]:
    metadata = dict_items(signal.get("metadata"))
    rows: list[dict[str, Any]] = []
    for metadata_field, family in (
        ("issue_labels", "formal_issue_labels"),
        ("concern_facets", "formal_concern_facets"),
    ):
        for label in unique_texts(list_items(metadata.get(metadata_field))):
            rows.append(
                {
                    "label_family": family,
                    "label": label,
                    "matched_cues": [],
                    "label_method": "formal-signal-metadata-candidate-taxonomy",
                }
            )
    stance = maybe_text(metadata.get("stance_hint"))
    if stance:
        rows.append(
            {
                "label_family": "formal_stance_hints",
                "label": stance,
                "matched_cues": [],
                "label_method": "formal-signal-metadata-candidate-taxonomy",
            }
        )
    return rows


def _candidate_labels_for_text(text: str, *, max_labels_per_family: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for family, family_rules in FORMAL_LABEL_FAMILY_RULES.items():
        family_count = 0
        for label, cues in family_rules.items():
            matches = _matched_cues(text, cues)
            if not matches:
                continue
            rows.append(
                {
                    "label_family": family,
                    "label": label,
                    "matched_cues": matches[:8],
                    "label_method": "formal-comment-keyword-rubric",
                }
            )
            seen.add((family, label))
            family_count += 1
            if max_labels_per_family > 0 and family_count >= max_labels_per_family:
                break
    for family, family_rules in FORMAL_SEMANTICS_FALLBACKS.items():
        family_count = sum(1 for row in rows if row["label_family"] == family)
        for label, cues in family_rules.items():
            if (family, label) in seen:
                continue
            matches = _matched_cues(text, cues)
            if not matches:
                continue
            rows.append(
                {
                    "label_family": family,
                    "label": label,
                    "matched_cues": matches[:8],
                    "label_method": "formal-public-taxonomy-fallback-rubric",
                }
            )
            seen.add((family, label))
            family_count += 1
            if max_labels_per_family > 0 and family_count >= max_labels_per_family:
                break
    return rows


def _selected_formal_signals(
    signals: list[dict[str, Any]],
    *,
    source_skill: str,
    signal_kind: str,
    limit: int,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    source_filter = maybe_text(source_skill)
    kind_filter = maybe_text(signal_kind)
    for signal in signals:
        current_source = maybe_text(signal.get("source_skill"))
        current_kind = maybe_text(signal.get("signal_kind"))
        if current_source not in FORMAL_COMMENT_SOURCE_SKILLS:
            continue
        if source_filter and current_source != source_filter:
            continue
        if kind_filter and current_kind != kind_filter:
            continue
        if current_kind and current_kind not in FORMAL_COMMENT_TEXT_KINDS:
            continue
        if not signal_text(signal):
            continue
        selected.append(signal)
        if len(selected) >= max(1, int(limit or 500)):
            break
    return selected


def _annotation_rows_for_signal(
    signal: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
    annotation_basis_ref: str,
    max_labels_per_family: int,
) -> list[dict[str, Any]]:
    signal_id = maybe_text(signal.get("signal_id"))
    if not signal_id:
        return []
    text = signal_text(signal)
    if not text:
        return []
    candidates = [
        *_metadata_label_rows(signal),
        *_candidate_labels_for_text(text, max_labels_per_family=max_labels_per_family),
    ]
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for candidate in candidates:
        family = maybe_text(candidate.get("label_family"))
        label = maybe_text(candidate.get("label"))
        if not family or not label or (family, label) in seen:
            continue
        seen.add((family, label))
        rows.append(
            {
                "annotation_id": "formal-comment-worker-annotation-"
                + stable_hash(run_id, round_id, signal_id, family, label, annotation_basis_ref)[:12],
                "signal_id": signal_id,
                "label_family": family,
                "label": label,
                "annotation_source": "formal-comment-issue-worker",
                "annotation_basis_ref": annotation_basis_ref,
                "audit_status": "worker-labeled",
                "source_skill": maybe_text(signal.get("source_skill")),
                "signal_kind": maybe_text(signal.get("signal_kind")),
                "text_excerpt": text[:500],
                "matched_cues": list_items(candidate.get("matched_cues")),
                "label_method": maybe_text(candidate.get("label_method")),
                "taxonomy_version": FORMAL_PUBLIC_TAXONOMY_VERSION,
                "evidence_refs": list_items(signal.get("evidence_refs")),
            }
        )
    return rows


def run_classify_formal_comment_issues(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    round_scope: str = "current",
    source_skill: str = "",
    signal_kind: str = "",
    annotation_basis_ref: str = "",
    output_path: str = "",
    max_items: int = 500,
    max_labels_per_family: int = 4,
) -> dict[str, Any]:
    skill_name = "classify-formal-comment-issues"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"formal_comment_issue_annotations_{round_id}.json")
    normalized_round_scope = "run" if maybe_text(round_scope).casefold() in {"run", "all", "all-run", "run-wide"} else "current"
    query_round_id = "" if normalized_round_scope == "run" else round_id
    all_formal_signals, db_path = query_signals(
        run_dir_path,
        run_id=run_id,
        round_id=query_round_id,
        plane="formal",
        limit=1000,
    )
    sample_signals = _selected_formal_signals(
        all_formal_signals,
        source_skill=source_skill,
        signal_kind=signal_kind,
        limit=max_items,
    )
    annotation_basis = maybe_text(annotation_basis_ref) or DEFAULT_ANNOTATION_BASIS_REF
    annotations: list[dict[str, Any]] = []
    for signal in sample_signals:
        annotations.extend(
            _annotation_rows_for_signal(
                signal,
                run_id=run_id,
                round_id=round_id,
                annotation_basis_ref=annotation_basis,
                max_labels_per_family=max_labels_per_family,
            )
        )
    warnings: list[dict[str, str]] = []
    if not sample_signals:
        warnings.append(
            {
                "code": "no-formal-comment-text-signals",
                "message": "No DB-visible formal comment text signals matched the selected filters.",
            }
        )
    elif not annotations:
        warnings.append(
            {
                "code": "no-formal-comment-annotations",
                "message": "Formal comment text was present, but the worker did not find configured issue, stance, or concern cues.",
            }
        )
    if source_skill and maybe_text(source_skill) not in FORMAL_COMMENT_SOURCE_SKILLS:
        warnings.append(
            {
                "code": "source-skill-outside-formal-comment-family",
                "message": f"Source skill {maybe_text(source_skill)} is outside the formal comment text source family.",
            }
        )
    metadata = helper_metadata(
        skill_name=skill_name,
        rule_trace=["formal-comment-sample-local-semantic-annotation"],
        caveats=[
            "Worker labels describe only DB-visible formal comment text signals in the selected sample.",
            "This helper does not infer stance distribution, representativeness, source importance, or evidence sufficiency.",
            "Report-facing use requires explicit council uptake and citation before downstream use.",
        ],
    )
    annotation_set_id = "formal-comment-issue-annotation-set-" + stable_hash(
        run_id,
        round_id,
        normalized_round_scope,
        source_skill,
        signal_kind,
        annotation_basis,
        len(annotations),
    )[:12]
    payload = {
        "schema_version": "optional-analysis-formal-comment-issue-classification-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "helper_governance": metadata,
        "annotation_set_id": annotation_set_id,
        "annotation_basis_ref": annotation_basis,
        "annotation_worker_role": "formal-comment-issue-worker",
        "annotation_worker_scope": {
            "role_kind": "bounded optional-analysis worker, not a council agent",
            "allowed_output": "item-level sample annotations for formal comment issue, stance, and concern cues",
            "forbidden_output": [
                "findings",
                "readiness decisions",
                "source ranking",
                "evidence sufficiency decisions",
                "population or public-opinion estimates",
            ],
        },
        "sample_definition": {
            "run_id": run_id,
            "round_id": round_id,
            "round_scope": normalized_round_scope,
            "sample_boundary": "DB-visible formal comment text signals only",
            "eligible_source_skills": sorted(FORMAL_COMMENT_SOURCE_SKILLS),
            "text_unit": "formal comment listing, detail, or attachment text signal",
            "label_families": sorted(FORMAL_LABEL_FAMILY_RULES),
            "labels_are_non_exclusive": True,
        },
        "sample_count": len(sample_signals),
        "annotation_count": len(annotations),
        "annotations": annotations,
        "representativeness_limits": [
            "Labels describe only the selected formal comment sample.",
            "Unannotated or unreadable attachments are not negative evidence for any label.",
            "Sample label counts are not general public-opinion estimates.",
        ],
        "observed_inputs": {
            "db_path": db_path,
            "formal_signal_count": len(all_formal_signals),
            "selected_signal_count": len(sample_signals),
        },
        "source_parameters": {
            "db_path": db_path,
            "eligible_source_skills": sorted(FORMAL_COMMENT_SOURCE_SKILLS),
            "source_skill_filter": maybe_text(source_skill),
            "signal_kind_filter": maybe_text(signal_kind),
            "max_items": int(max_items or 500),
            "max_labels_per_family": int(max_labels_per_family or 4),
        },
        "query_parameters": {
            "run_id": run_id,
            "round_id": round_id,
            "round_scope": normalized_round_scope,
            "annotation_basis_ref": annotation_basis,
        },
        "artifact_refs": [artifact_ref(output_file, "$.annotations")],
        "provenance": {
            "source_skill": skill_name,
            "decision_source": metadata["decision_source"],
            "db_path": db_path,
            "taxonomy_version": FORMAL_PUBLIC_TAXONOMY_VERSION,
        },
        "warnings": warnings,
    }
    write_json(output_file, payload)
    return {
        "status": "completed",
        "summary": {
            "skill": skill_name,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "sample_count": len(sample_signals),
            "annotation_count": len(annotations),
            "decision_source": metadata["decision_source"],
            "rule_id": metadata["rule_id"],
        },
        "receipt_id": "formal-comment-issues-receipt-"
        + stable_hash(skill_name, run_id, round_id, output_file, annotation_set_id)[:20],
        "batch_id": "formal-comment-issues-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.annotations")],
        "canonical_ids": [annotation_set_id],
        "warnings": warnings,
        "annotation_set_id": annotation_set_id,
        "board_handoff": safe_board_handoff(
            artifact_path=output_file,
            locator="$.annotations",
            candidate_ids=[annotation_set_id],
            gap_hints=[warning["message"] for warning in warnings],
        ),
    }
