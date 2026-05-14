from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any

from .research_issues import (
    approved_helper_input_payload,
    load_json_file,
    signal_text,
    unapproved_input_warning,
)
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
    signal_within_time_filter,
    stable_hash,
    unique_texts,
    utc_now_iso,
    write_json,
)


__all__ = (
    "PUBLIC_DISCOURSE_SOURCE_FAMILY_BY_SKILL",
    "public_discourse_lane",
    "public_discourse_source_family",
    "run_aggregate_public_discourse_annotations",
    "run_audit_public_discourse_sample_coverage",
    "run_compare_public_media_narratives",
    "run_materialize_public_discourse_corpus",
    "run_summarize_public_discourse_sample",
)


PUBLIC_DISCOURSE_SOURCE_FAMILY_BY_SKILL = {
    "fetch-gdelt-doc-search": "gdelt-public-record",
    "fetch-gdelt-events": "gdelt-public-record",
    "fetch-gdelt-mentions": "gdelt-public-record",
    "fetch-gdelt-gkg": "gdelt-public-record",
    "fetch-youtube-video-search": "youtube-public-discourse",
    "fetch-youtube-comments": "youtube-public-discourse",
    "fetch-bluesky-cascade": "bluesky-public-discourse",
    "fetch-regulationsgov-comments": "regulationsgov-formal-comments",
    "fetch-regulationsgov-comment-detail": "regulationsgov-formal-comments",
}

PUBLIC_DISCOURSE_EXPECTED_FAMILIES = [
    {
        "source_family": "youtube-public-discourse",
        "text_sample_skills": ["fetch-youtube-comments"],
        "recon_skills": ["fetch-youtube-video-search"],
    },
    {
        "source_family": "bluesky-public-discourse",
        "text_sample_skills": ["fetch-bluesky-cascade"],
        "recon_skills": [],
    },
    {
        "source_family": "gdelt-public-record",
        "text_sample_skills": [
            "fetch-gdelt-doc-search",
            "fetch-gdelt-events",
            "fetch-gdelt-mentions",
            "fetch-gdelt-gkg",
        ],
        "recon_skills": ["fetch-gdelt-doc-search"],
    },
    {
        "source_family": "regulationsgov-formal-comments",
        "text_sample_skills": [
            "fetch-regulationsgov-comments",
            "fetch-regulationsgov-comment-detail",
        ],
        "recon_skills": ["fetch-regulationsgov-comments"],
    },
]

SOCIAL_SAMPLE_AFFECT_SKILLS = {
    "fetch-youtube-comments",
    "fetch-bluesky-cascade",
}
FORMAL_COMMENT_SAMPLE_SKILLS = {
    "fetch-regulationsgov-comments",
    "fetch-regulationsgov-comment-detail",
}
GDELT_MEDIA_TONE_SKILLS = {
    "fetch-gdelt-events",
    "fetch-gdelt-mentions",
    "fetch-gdelt-gkg",
}
GDELT_TONE_LANES = {
    "gdelt_media_tone",
    "gdelt_doc_tone_aggregate",
}
ANNOTATION_LABEL_FAMILIES = {
    "issue_facets",
    "affect_labels",
    "source_narrative_labels",
    "actor_responsibility_labels",
    "action_orientation_labels",
}
ANNOTATION_FAMILY_ALIASES = {
    "issue": "issue_facets",
    "issues": "issue_facets",
    "issue_facet": "issue_facets",
    "issue_facets": "issue_facets",
    "affect": "affect_labels",
    "affect_label": "affect_labels",
    "affect_labels": "affect_labels",
    "sentiment": "affect_labels",
    "source_narrative": "source_narrative_labels",
    "source_narrative_label": "source_narrative_labels",
    "source_narrative_labels": "source_narrative_labels",
    "actor_responsibility": "actor_responsibility_labels",
    "actor_responsibility_label": "actor_responsibility_labels",
    "actor_responsibility_labels": "actor_responsibility_labels",
    "action_orientation": "action_orientation_labels",
    "action_orientation_label": "action_orientation_labels",
    "action_orientation_labels": "action_orientation_labels",
}


def public_discourse_source_family(signal: dict[str, Any]) -> str:
    source_skill = maybe_text(signal.get("source_skill"))
    if source_skill in PUBLIC_DISCOURSE_SOURCE_FAMILY_BY_SKILL:
        return PUBLIC_DISCOURSE_SOURCE_FAMILY_BY_SKILL[source_skill]
    plane = maybe_text(signal.get("plane"))
    if plane == "formal":
        return "formal-record"
    if plane == "public":
        return "public-discourse"
    return "other"


def public_discourse_lane(signal: dict[str, Any]) -> str:
    source_skill = maybe_text(signal.get("source_skill"))
    if source_skill in SOCIAL_SAMPLE_AFFECT_SKILLS:
        return "social_sample_affect"
    if source_skill in FORMAL_COMMENT_SAMPLE_SKILLS:
        return "formal_public_comment_sample"
    if source_skill in GDELT_MEDIA_TONE_SKILLS:
        return "gdelt_media_tone"
    if source_skill == "fetch-gdelt-doc-search":
        metadata = dict_items(signal.get("metadata"))
        metric = maybe_text(signal.get("metric"))
        gdelt_doc_kind = maybe_text(metadata.get("gdelt_doc_kind"))
        if (
            gdelt_doc_kind in {"gdelt_doc_tone_aggregate", "gdelt_doc_tone_distribution"}
            or metric in {"doc_timeline_tone", "doc_tonechart_count"}
            or maybe_text(metadata.get("doc_mode")) in {"timelinetone", "tonechart"}
        ):
            return "gdelt_doc_tone_aggregate"
        return "gdelt_doc_recon"
    if source_skill == "fetch-youtube-video-search":
        return "public_visibility"
    if maybe_text(signal.get("plane")) == "formal":
        return "formal_record_text"
    return "public_discourse_text"


def _source_family_counts(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter(public_discourse_source_family(signal) for signal in signals)
    return [
        {"source_family": source_family, "signal_count": count}
        for source_family, count in sorted(counts.items())
        if source_family
    ]


def _source_skill_counts(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter(maybe_text(signal.get("source_skill")) for signal in signals)
    return [
        {"source_skill": source_skill, "signal_count": count}
        for source_skill, count in sorted(counts.items())
        if source_skill
    ]


def _lane_counts(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter(public_discourse_lane(signal) for signal in signals)
    return [
        {"discourse_lane": lane, "signal_count": count}
        for lane, count in sorted(counts.items())
        if lane
    ]


def _keyword_match(signal: dict[str, Any], keywords: list[str]) -> bool:
    terms = [keyword.casefold() for keyword in unique_texts(keywords)]
    if not terms:
        return True
    text = signal_text(signal).casefold()
    return any(term in text for term in terms)


def _filter_discourse_signals(
    signals: list[dict[str, Any]],
    *,
    source_family: str,
    source_skill: str,
    keyword_any: list[str],
    observed_after_utc: str,
    observed_before_utc: str,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for signal in signals:
        if maybe_text(source_family) and public_discourse_source_family(signal) != maybe_text(source_family):
            continue
        if maybe_text(source_skill) and maybe_text(signal.get("source_skill")) != maybe_text(source_skill):
            continue
        if not _keyword_match(signal, keyword_any):
            continue
        if not signal_within_time_filter(
            signal,
            observed_after_utc=observed_after_utc,
            observed_before_utc=observed_before_utc,
        ):
            continue
        if not signal_text(signal):
            continue
        selected.append(signal)
    return selected


def _normalized_round_scope(value: Any) -> str:
    text = maybe_text(value).casefold()
    if text in {"run", "all", "all-run", "run-wide"}:
        return "run"
    return "current"


def _load_discourse_signals(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    round_scope: str = "current",
    limit: int,
) -> tuple[list[dict[str, Any]], str]:
    run_dir_path = resolve_run_dir(run_dir)
    normalized_scope = _normalized_round_scope(round_scope)
    query_round_id = "" if normalized_scope == "run" else round_id
    public_signals, db_path = query_signals(
        run_dir_path,
        run_id=run_id,
        round_id=query_round_id,
        plane="public",
        limit=limit,
    )
    formal_signals, _ = query_signals(
        run_dir_path,
        run_id=run_id,
        round_id=query_round_id,
        plane="formal",
        limit=limit,
    )
    return [*public_signals, *formal_signals], db_path


def _corpus_items(signals: list[dict[str, Any]], *, run_id: str, round_id: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for signal in signals:
        text = signal_text(signal)
        signal_id = maybe_text(signal.get("signal_id"))
        item_id = "public-discourse-corpus-item-" + stable_hash(run_id, round_id, signal_id)[:12]
        metadata = dict_items(signal.get("metadata"))
        items.append(
            {
                "corpus_item_id": item_id,
                "signal_id": signal_id,
                "run_id": run_id,
                "round_id": maybe_text(signal.get("round_id")) or round_id,
                "source_family": public_discourse_source_family(signal),
                "source_skill": maybe_text(signal.get("source_skill")),
                "discourse_lane": public_discourse_lane(signal),
                "signal_kind": maybe_text(signal.get("signal_kind")),
                "title": maybe_text(signal.get("title")),
                "text_excerpt": text[:500],
                "published_at_utc": maybe_text(signal.get("published_at_utc")),
                "observed_at_utc": maybe_text(signal.get("observed_at_utc")),
                "first_timestamp_utc": first_timestamp(signal),
                "author_name": maybe_text(signal.get("author_name")),
                "channel_name": maybe_text(signal.get("channel_name")),
                "language": maybe_text(signal.get("language")) or maybe_text(metadata.get("language")),
                "metric": maybe_text(signal.get("metric")),
                "numeric_value": signal.get("numeric_value"),
                "evidence_refs": list_items(signal.get("evidence_refs")),
                "lineage": [signal_id],
                "provenance": {
                    "source_skill": maybe_text(signal.get("source_skill")),
                    "artifact_path": maybe_text(list_items(signal.get("evidence_refs"))[0].get("artifact_path"))
                    if list_items(signal.get("evidence_refs")) and isinstance(list_items(signal.get("evidence_refs"))[0], dict)
                    else "",
                },
            }
        )
    return items


def _public_discourse_metadata(skill_name: str) -> dict[str, Any]:
    return helper_metadata(
        skill_name=skill_name,
        rule_trace=["db-public-formal-signal-sample-boundary"],
        caveats=[
            "This helper describes a DB-visible sample only and cannot infer general public opinion.",
            "GDELT tone signals describe media/document tone, not public response sentiment.",
            "Report use requires uptake through DB council or reporting basis objects.",
        ],
    )


def _distribution_use_policy() -> dict[str, Any]:
    return {
        "schema_version": "public-discourse-distribution-use-policy-v1",
        "label_sets_are_non_exclusive": True,
        "sample_fractions_are_sample_local": True,
        "do_not_sum_to_population_opinion": True,
        "requires_council_uptake_before_reporting": True,
        "gdelt_tone_boundary": "media_or_document_tone_not_public_sentiment",
        "source_narrative_boundary": "public_source_narrative_cue_not_physical_source_attribution",
    }


def _load_corpus_payload(corpus_path: str) -> tuple[dict[str, Any], list[dict[str, str]]]:
    if not maybe_text(corpus_path):
        return {}, []
    payload = load_json_file(corpus_path, {})
    approved, reason = approved_helper_input_payload(
        payload,
        allowed_skills={"materialize-public-discourse-corpus"},
    )
    if not approved:
        return {}, [unapproved_input_warning(corpus_path, reason)]
    return payload, []


def _signals_from_corpus_or_db(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    round_scope: str = "current",
    corpus_path: str,
    limit: int,
) -> tuple[list[dict[str, Any]], dict[str, Any], str, list[dict[str, str]]]:
    corpus_payload, warnings = _load_corpus_payload(corpus_path)
    source_signals, db_path = _load_discourse_signals(
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        round_scope=round_scope,
        limit=limit,
    )
    if not corpus_payload:
        return source_signals, {}, db_path, warnings
    corpus_items = [
        dict(item)
        for item in list_items(corpus_payload.get("corpus_items"))
        if isinstance(item, dict) and maybe_text(item.get("signal_id"))
    ]
    if not corpus_items:
        return [], corpus_payload, db_path, warnings
    return corpus_items[: max(1, int(limit or 500))], corpus_payload, db_path, warnings


def _annotation_family(value: Any) -> str:
    text = maybe_text(value).strip().lower().replace("-", "_").replace(" ", "_")
    return ANNOTATION_FAMILY_ALIASES.get(text, text if text in ANNOTATION_LABEL_FAMILIES else "")


def _annotation_labels(value: Any) -> list[str]:
    if isinstance(value, list):
        return unique_texts(value)
    text = maybe_text(value)
    if not text:
        return []
    return unique_texts([item.strip() for item in text.split(",")])


def _read_annotation_payload(path_text: str) -> Any:
    text = maybe_text(path_text)
    if not text:
        return []
    path = Path(text).expanduser()
    if not path.exists():
        return []
    raw = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".jsonl":
        rows: list[Any] = []
        for line in raw.splitlines():
            if not line.strip():
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return rows
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return []


def _annotation_rows_from_path(
    annotations_path: str,
    *,
    annotation_basis_ref: str,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    if not maybe_text(annotations_path):
        return [], []
    payload = _read_annotation_payload(annotations_path)
    artifact_basis_ref = maybe_text(payload.get("annotation_basis_ref")) if isinstance(payload, dict) else ""
    effective_basis_ref = maybe_text(annotation_basis_ref) or artifact_basis_ref
    if not effective_basis_ref:
        return [], [
            {
                "code": "annotation-basis-required",
                "message": "Annotations require --annotation-basis-ref or artifact.annotation_basis_ref before aggregation.",
            }
        ]
    if isinstance(payload, dict):
        raw_rows = list_items(payload.get("annotations") or payload.get("labels"))
    elif isinstance(payload, list):
        raw_rows = payload
    else:
        raw_rows = []
    rows: list[dict[str, Any]] = []
    warnings: list[dict[str, str]] = []
    for index, raw_row in enumerate(raw_rows):
        if not isinstance(raw_row, dict):
            continue
        signal_id = maybe_text(raw_row.get("signal_id"))
        family = _annotation_family(
            raw_row.get("label_family")
            or raw_row.get("family")
            or raw_row.get("annotation_family")
        )
        labels = _annotation_labels(raw_row.get("label") or raw_row.get("labels"))
        if not signal_id or not family or not labels:
            warnings.append(
                {
                    "code": "annotation-row-skipped",
                    "message": f"Skipped annotation row {index}; signal_id, label_family, and label are required.",
                }
            )
            continue
        for label in labels:
            rows.append(
                {
                    "annotation_id": maybe_text(raw_row.get("annotation_id"))
                    or "public-discourse-annotation-" + stable_hash(annotations_path, index, signal_id, family, label)[:12],
                    "signal_id": signal_id,
                    "label_family": family,
                    "label": label,
                    "annotation_source": maybe_text(raw_row.get("annotation_source")) or "agent-authored-annotation",
                    "annotation_basis_ref": maybe_text(raw_row.get("annotation_basis_ref")) or effective_basis_ref,
                    "audit_status": maybe_text(raw_row.get("audit_status")) or "candidate-for-human-review",
                    "evidence_refs": list_items(raw_row.get("evidence_refs")),
                }
            )
    if not rows:
        warnings.append(
            {
                "code": "no-agent-annotations-loaded",
                "message": "No agent-authored annotation rows were loaded from the supplied annotations path.",
            }
        )
    return rows, warnings


def _taxonomy_annotation_rows(
    taxonomy_labels_path: str,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    if not maybe_text(taxonomy_labels_path):
        return [], []
    payload = load_json_file(taxonomy_labels_path, {})
    approved, reason = approved_helper_input_payload(
        payload,
        allowed_skills={"apply-approved-formal-public-taxonomy"},
    )
    if not approved:
        return [], [unapproved_input_warning(taxonomy_labels_path, reason)]
    rows: list[dict[str, Any]] = []
    for cue in list_items(payload.get("taxonomy_labels")):
        if not isinstance(cue, dict):
            continue
        signal_id = maybe_text(cue.get("signal_id"))
        for label in unique_texts(list_items(cue.get("candidate_labels"))):
            rows.append(
                {
                    "annotation_id": "public-discourse-taxonomy-annotation-" + stable_hash(signal_id, label, taxonomy_labels_path)[:12],
                    "signal_id": signal_id,
                    "label_family": "issue_facets",
                    "label": label,
                    "annotation_source": "approved-taxonomy-label-cue",
                    "annotation_basis_ref": maybe_text(cue.get("taxonomy_approval_ref")),
                    "audit_status": maybe_text(cue.get("audit_status")) or "candidate-for-human-review",
                    "evidence_refs": list_items(cue.get("evidence_refs")),
                }
            )
    warnings = [] if rows else [
        {
            "code": "no-taxonomy-annotations-loaded",
            "message": "The approved taxonomy artifact did not contain candidate label cues.",
        }
    ]
    return rows, warnings


def _distribution_record(
    *,
    run_id: str,
    round_id: str,
    label_family: str,
    label: str,
    annotations: list[dict[str, Any]],
    signal_lookup: dict[str, dict[str, Any]],
    total_annotated_signals: int,
) -> dict[str, Any]:
    signal_ids = unique_texts([annotation.get("signal_id") for annotation in annotations])
    signals = [signal_lookup[signal_id] for signal_id in signal_ids if signal_id in signal_lookup]
    count = len(signal_ids)
    sample_fraction = round(count / total_annotated_signals, 6) if total_annotated_signals else 0.0
    return {
        "distribution_id": "public-discourse-annotation-distribution-" + stable_hash(run_id, round_id, label_family, label)[:12],
        "label_family": label_family,
        "label": label,
        "annotated_signal_count": count,
        "sample_fraction": sample_fraction,
        "source_family_counts": _source_family_counts(signals),
        "discourse_lane_counts": _lane_counts(signals),
        "signal_ids": signal_ids[:50],
        "evidence_refs": refs_from_signals(signals),
        "lineage": lineage_from_signals(signals),
        "audit_status": "candidate-for-human-review",
        "provenance": {
            "annotation_sources": unique_texts([annotation.get("annotation_source") for annotation in annotations]),
            "annotation_basis_refs": unique_texts([annotation.get("annotation_basis_ref") for annotation in annotations]),
        },
    }


def _empty_family_distribution(label_family: str) -> list[dict[str, Any]]:
    return []


def run_aggregate_public_discourse_annotations(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    round_scope: str = "current",
    corpus_path: str = "",
    annotations_path: str = "",
    taxonomy_labels_path: str = "",
    annotation_basis_ref: str = "",
    output_path: str = "",
    limit: int = 500,
) -> dict[str, Any]:
    skill_name = "aggregate-public-discourse-annotations"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"public_discourse_annotation_aggregation_{round_id}.json")
    normalized_round_scope = _normalized_round_scope(round_scope)
    signals, corpus_payload, db_path, corpus_warnings = _signals_from_corpus_or_db(
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        round_scope=normalized_round_scope,
        corpus_path=corpus_path,
        limit=limit,
    )
    signal_lookup = {maybe_text(signal.get("signal_id")): signal for signal in signals}
    annotation_rows, annotation_warnings = _annotation_rows_from_path(
        annotations_path,
        annotation_basis_ref=annotation_basis_ref,
    )
    taxonomy_rows, taxonomy_warnings = _taxonomy_annotation_rows(taxonomy_labels_path)
    all_rows = [*annotation_rows, *taxonomy_rows]
    matched_rows: list[dict[str, Any]] = []
    skipped_outside = 0
    for row in all_rows:
        signal_id = maybe_text(row.get("signal_id"))
        if signal_id not in signal_lookup:
            skipped_outside += 1
            continue
        matched_rows.append(row)
    metadata = _public_discourse_metadata(skill_name)
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in matched_rows:
        grouped.setdefault((maybe_text(row.get("label_family")), maybe_text(row.get("label"))), []).append(row)
    annotated_signal_ids = unique_texts([row.get("signal_id") for row in matched_rows])
    total_annotated_signals = len(annotated_signal_ids)
    annotated_signal_count_by_family = {
        family: len(unique_texts([row.get("signal_id") for row in matched_rows if maybe_text(row.get("label_family")) == family]))
        for family in ANNOTATION_LABEL_FAMILIES
    }
    distribution_records = [
        _distribution_record(
            run_id=run_id,
            round_id=round_id,
            label_family=label_family,
            label=label,
            annotations=rows,
            signal_lookup=signal_lookup,
            total_annotated_signals=annotated_signal_count_by_family.get(label_family, 0),
        )
        for (label_family, label), rows in sorted(grouped.items())
        if label_family in ANNOTATION_LABEL_FAMILIES and label
    ]
    distributions_by_family = {
        family: [
            item
            for item in distribution_records
            if maybe_text(item.get("label_family")) == family
        ]
        or _empty_family_distribution(family)
        for family in sorted(ANNOTATION_LABEL_FAMILIES)
    }
    warnings = [
        *corpus_warnings,
        *annotation_warnings,
        *taxonomy_warnings,
    ]
    if skipped_outside:
        warnings.append(
            {
                "code": "annotation-signals-outside-sample",
                "message": f"Skipped {skipped_outside} annotations whose signal_id was not present in the selected corpus or DB sample.",
            }
        )
    if not matched_rows:
        warnings.append(
            {
                "code": "no-annotations-aggregated",
                "message": "No approved taxonomy cues or agent-authored annotations matched the selected public discourse sample.",
            }
        )
    aggregation_id = "public-discourse-annotation-aggregation-" + stable_hash(
        run_id,
        round_id,
        normalized_round_scope,
        corpus_path,
        annotations_path,
        taxonomy_labels_path,
        total_annotated_signals,
    )[:12]
    sample_definition = dict_items(corpus_payload.get("sample_definition")) if corpus_payload else {
        "run_id": run_id,
        "round_id": round_id,
        "round_scope": normalized_round_scope,
        "sample_boundary": "DB-visible normalized public/formal text sample only",
    }
    payload = {
        "schema_version": "optional-analysis-public-discourse-annotation-aggregation-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "helper_governance": metadata,
        "aggregation_id": aggregation_id,
        "sample_definition": sample_definition,
        "sample_count": len(signals),
        "annotation_count": len(matched_rows),
        "annotated_signal_count": total_annotated_signals,
        "issue_distribution": distributions_by_family["issue_facets"],
        "social_affect_distribution": distributions_by_family["affect_labels"],
        "source_narrative_distribution": distributions_by_family["source_narrative_labels"],
        "actor_responsibility_distribution": distributions_by_family["actor_responsibility_labels"],
        "action_orientation_distribution": distributions_by_family["action_orientation_labels"],
        "annotation_distributions": distribution_records,
        "distribution_use_policy": _distribution_use_policy(),
        "representativeness_limits": [
            "Annotation distributions describe only annotated items inside the selected sample.",
            "Unannotated items are not negative evidence for any label.",
            "Sample fractions are within-sample descriptors, not public-opinion estimates.",
        ],
        "observed_inputs": {
            "db_path": db_path,
            "round_scope": normalized_round_scope,
            "corpus_path": maybe_text(corpus_path),
            "annotations_path": maybe_text(annotations_path),
            "taxonomy_labels_path": maybe_text(taxonomy_labels_path),
        },
        "source_parameters": {"db_path": db_path},
        "query_parameters": {
            "run_id": run_id,
            "round_id": round_id,
            "round_scope": normalized_round_scope,
            "corpus_path": maybe_text(corpus_path),
            "annotation_basis_ref": maybe_text(annotation_basis_ref),
        },
        "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"], "db_path": db_path},
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
            "annotation_count": len(matched_rows),
            "distribution_count": len(distribution_records),
            "decision_source": metadata["decision_source"],
            "rule_id": metadata["rule_id"],
        },
        "receipt_id": "public-discourse-annotations-receipt-"
        + stable_hash(skill_name, run_id, round_id, output_file, aggregation_id)[:20],
        "batch_id": "public-discourse-annotations-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.annotation_distributions")],
        "canonical_ids": [aggregation_id],
        "warnings": warnings,
        "aggregation_id": aggregation_id,
        "board_handoff": safe_board_handoff(
            artifact_path=output_file,
            locator="$.annotation_distributions",
            candidate_ids=[aggregation_id],
            gap_hints=[warning["message"] for warning in warnings],
        ),
    }


def _load_approved_aggregation(aggregation_path: str) -> tuple[dict[str, Any], list[dict[str, str]]]:
    if not maybe_text(aggregation_path):
        return {}, []
    payload = load_json_file(aggregation_path, {})
    approved, reason = approved_helper_input_payload(
        payload,
        allowed_skills={"aggregate-public-discourse-annotations"},
    )
    if not approved:
        return {}, [unapproved_input_warning(aggregation_path, reason)]
    return payload, []


def _load_approved_helper_artifact(
    path_text: str,
    *,
    allowed_skills: set[str],
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    if not maybe_text(path_text):
        return {}, []
    payload = load_json_file(path_text, {})
    approved, reason = approved_helper_input_payload(
        payload,
        allowed_skills=allowed_skills,
    )
    if not approved:
        return {}, [unapproved_input_warning(path_text, reason)]
    return payload, []


def _numeric_metric_summary(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, list[float]] = {}
    for signal in signals:
        if public_discourse_lane(signal) not in GDELT_TONE_LANES:
            continue
        metric = maybe_text(signal.get("metric")) or "unspecified"
        value = signal.get("numeric_value")
        if not isinstance(value, (int, float)):
            continue
        buckets.setdefault(metric, []).append(float(value))
    summaries: list[dict[str, Any]] = []
    for metric, values in sorted(buckets.items()):
        summaries.append(
            {
                "metric": metric,
                "numeric_count": len(values),
                "min_value": min(values),
                "max_value": max(values),
                "average_value": round(sum(values) / len(values), 6),
                "tone_boundary": "gdelt_media_or_doc_tone_not_public_response_sentiment",
            }
        )
    return summaries


def _distribution_labels(distributions: list[Any]) -> set[str]:
    return {
        maybe_text(item.get("label"))
        for item in distributions
        if isinstance(item, dict) and maybe_text(item.get("label"))
    }


def _source_narrative_cross_lane_cues(aggregation_payload: dict[str, Any]) -> list[dict[str, Any]]:
    cues: list[dict[str, Any]] = []
    for item in list_items(aggregation_payload.get("source_narrative_distribution")):
        if not isinstance(item, dict):
            continue
        lane_counts = [
            lane
            for lane in list_items(item.get("discourse_lane_counts"))
            if isinstance(lane, dict) and int(lane.get("signal_count") or 0) > 0
        ]
        lanes = unique_texts([lane.get("discourse_lane") for lane in lane_counts])
        cues.append(
            {
                "cue_id": "public-media-source-narrative-cue-" + stable_hash(item.get("distribution_id"), lanes)[:12],
                "label": maybe_text(item.get("label")),
                "observed_discourse_lanes": lanes,
                "annotated_signal_count": item.get("annotated_signal_count"),
                "comparison_status": "same-label-observed-across-sample-lanes" if len(lanes) > 1 else "single-sample-lane-observed",
                "evidence_refs": list_items(item.get("evidence_refs")),
                "lineage": list_items(item.get("lineage")),
                "audit_status": "candidate-for-human-review",
            }
        )
    return cues


def run_compare_public_media_narratives(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    round_scope: str = "current",
    corpus_path: str = "",
    aggregation_path: str = "",
    output_path: str = "",
    limit: int = 500,
) -> dict[str, Any]:
    skill_name = "compare-public-media-narratives"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"public_media_narrative_comparison_{round_id}.json")
    normalized_round_scope = _normalized_round_scope(round_scope)
    signals, corpus_payload, db_path, corpus_warnings = _signals_from_corpus_or_db(
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        round_scope=normalized_round_scope,
        corpus_path=corpus_path,
        limit=limit,
    )
    aggregation_payload, aggregation_warnings = _load_approved_aggregation(aggregation_path)
    metadata = _public_discourse_metadata(skill_name)
    lane_count_map = {
        maybe_text(item.get("discourse_lane")): int(item.get("signal_count") or 0)
        for item in _lane_counts(signals)
    }
    affect_labels = _distribution_labels(list_items(aggregation_payload.get("social_affect_distribution")))
    source_narrative_cues = _source_narrative_cross_lane_cues(aggregation_payload)
    warnings = [*corpus_warnings, *aggregation_warnings]
    if lane_count_map.get("gdelt_media_tone", 0):
        warnings.append(
            {
                "code": "gdelt-tone-boundary",
                "message": "GDELT tone is media/document tone and must not be read as public sentiment.",
            }
        )
    if lane_count_map.get("gdelt_doc_tone_aggregate", 0):
        warnings.append(
            {
                "code": "gdelt-doc-tone-boundary",
                "message": "GDELT DOC tone/tonechart/timelinetone values are media/document tone aggregates, not public sentiment.",
            }
        )
    if not lane_count_map.get("social_sample_affect", 0):
        warnings.append(
            {
                "code": "no-social-sample-affect-basis",
                "message": "No YouTube comments or Bluesky posts are present in the selected sample.",
            }
        )
    if not aggregation_payload:
        warnings.append(
            {
                "code": "annotation-aggregation-missing",
                "message": "No approved annotation aggregation artifact was supplied; comparison is limited to source-family and tone coverage cues.",
            }
        )
    comparison_id = "public-media-narrative-comparison-" + stable_hash(
        run_id,
        round_id,
        normalized_round_scope,
        corpus_path,
        aggregation_path,
        len(signals),
    )[:12]
    sample_definition = dict_items(corpus_payload.get("sample_definition")) if corpus_payload else {
        "run_id": run_id,
        "round_id": round_id,
        "round_scope": normalized_round_scope,
        "sample_boundary": "DB-visible normalized public/formal text sample only",
    }
    payload = {
        "schema_version": "optional-analysis-public-media-narrative-comparison-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "helper_governance": metadata,
        "comparison_id": comparison_id,
        "sample_definition": sample_definition,
        "sample_count": len(signals),
        "discourse_lane_counts": _lane_counts(signals),
        "source_family_counts": _source_family_counts(signals),
        "gdelt_media_tone_summary": _numeric_metric_summary(signals),
        "social_sample_affect_summary": {
            "annotated_affect_labels": sorted(affect_labels),
            "source": "annotation_aggregation" if aggregation_payload else "not-supplied",
            "boundary": "sample_affect_not_public_opinion",
        },
        "formal_comment_summary": {
            "signal_count": lane_count_map.get("formal_public_comment_sample", 0),
            "boundary": "formal_comments_are_policy_record_samples_not_general_public_opinion",
        },
        "source_narrative_cross_lane_cues": source_narrative_cues,
        "cross_source_comparison": {
            "comparison_scope": "advisory comparison of sample lanes only",
            "can_support": [
                "describing whether annotated labels appear in one or more sampled lanes",
                "separating GDELT media tone from social sample affect",
                "identifying follow-up questions for council review",
            ],
            "cannot_support": [
                "general public opinion estimates",
                "physical source attribution",
                "proof that narratives are true or false",
            ],
        },
        "representativeness_limits": [
            "Cross-source cues compare sampled and annotated records only.",
            "Agreement across sampled text lanes is not physical source proof.",
            "Sparse or missing lanes should trigger source-owner review before limiting claims.",
        ],
        "observed_inputs": {
            "db_path": db_path,
            "round_scope": normalized_round_scope,
            "corpus_path": maybe_text(corpus_path),
            "aggregation_path": maybe_text(aggregation_path),
        },
        "source_parameters": {"db_path": db_path},
        "query_parameters": {
            "run_id": run_id,
            "round_id": round_id,
            "round_scope": normalized_round_scope,
            "corpus_path": maybe_text(corpus_path),
            "aggregation_path": maybe_text(aggregation_path),
        },
        "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"], "db_path": db_path},
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
            "comparison_cue_count": len(source_narrative_cues),
            "decision_source": metadata["decision_source"],
            "rule_id": metadata["rule_id"],
        },
        "receipt_id": "public-media-narratives-receipt-"
        + stable_hash(skill_name, run_id, round_id, output_file, comparison_id)[:20],
        "batch_id": "public-media-narratives-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.source_narrative_cross_lane_cues")],
        "canonical_ids": [comparison_id],
        "warnings": warnings,
        "comparison_id": comparison_id,
        "board_handoff": safe_board_handoff(
            artifact_path=output_file,
            locator="$.source_narrative_cross_lane_cues",
            candidate_ids=[comparison_id],
            gap_hints=[warning["message"] for warning in warnings],
        ),
    }


def _sample_examples(
    *,
    signals: list[dict[str, Any]],
    corpus_payload: dict[str, Any],
    limit: int,
) -> list[dict[str, Any]]:
    corpus_items = [
        item
        for item in list_items(corpus_payload.get("corpus_items"))
        if isinstance(item, dict)
    ]
    if corpus_items:
        return [
            {
                "example_id": maybe_text(item.get("corpus_item_id")) or maybe_text(item.get("signal_id")),
                "signal_id": maybe_text(item.get("signal_id")),
                "source_family": maybe_text(item.get("source_family")),
                "discourse_lane": maybe_text(item.get("discourse_lane")),
                "text_excerpt": maybe_text(item.get("text_excerpt"))[:280],
                "evidence_refs": list_items(item.get("evidence_refs")),
                "lineage": list_items(item.get("lineage")),
            }
            for item in corpus_items[: max(1, limit)]
        ]
    return [
        {
            "example_id": "public-discourse-example-" + stable_hash(signal.get("signal_id"))[:12],
            "signal_id": maybe_text(signal.get("signal_id")),
            "source_family": public_discourse_source_family(signal),
            "discourse_lane": public_discourse_lane(signal),
            "text_excerpt": signal_text(signal)[:280],
            "evidence_refs": list_items(signal.get("evidence_refs")),
            "lineage": [maybe_text(signal.get("signal_id"))],
        }
        for signal in signals[: max(1, limit)]
    ]


def _artifact_warnings(*payloads: dict[str, Any]) -> list[dict[str, str]]:
    warnings: list[dict[str, str]] = []
    for payload in payloads:
        for warning in list_items(payload.get("warnings")):
            if isinstance(warning, dict):
                warnings.append(
                    {
                        "code": maybe_text(warning.get("code")) or "input-warning",
                        "message": maybe_text(warning.get("message")),
                    }
                )
    return warnings


def _artifact_representativeness_limits(*payloads: dict[str, Any]) -> list[str]:
    values: list[Any] = []
    for payload in payloads:
        values.extend(list_items(payload.get("representativeness_limits")))
    values.extend(
        [
            "This summary is advisory and must be carried by a council object before downstream use.",
            "Sample distributions are not public-opinion estimates.",
            "GDELT media/document tone, social sample affect, formal comments, and physical source attribution remain separate.",
        ]
    )
    return unique_texts(values)


def _distribution_from_payload(payload: dict[str, Any], field_name: str) -> list[Any]:
    return list_items(payload.get(field_name)) if payload else []


def _summary_board_handoff(
    *,
    output_file: Path,
    summary_id: str,
    warnings: list[dict[str, str]],
) -> dict[str, Any]:
    handoff = safe_board_handoff(
        artifact_path=output_file,
        locator="$",
        candidate_ids=[summary_id],
        gap_hints=[warning["message"] for warning in warnings if maybe_text(warning.get("message"))],
        challenge_hints=[
            "Review sample definition, annotation basis, source-family coverage, GDELT tone separation, and report wording before citing this summary."
        ],
    )
    handoff["suggested_next_skills"] = [
        "submit-agent-position",
        "submit-evidence-request",
        "submit-council-proposal",
        "submit-readiness-opinion",
        "submit-round-synthesis",
    ]
    return handoff


def run_summarize_public_discourse_sample(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    round_scope: str = "current",
    corpus_path: str = "",
    coverage_audit_path: str = "",
    aggregation_path: str = "",
    comparison_path: str = "",
    output_path: str = "",
    limit: int = 500,
    example_limit: int = 8,
) -> dict[str, Any]:
    skill_name = "summarize-public-discourse-sample"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"public_discourse_sample_summary_{round_id}.json")
    normalized_round_scope = _normalized_round_scope(round_scope)
    signals, corpus_payload, db_path, corpus_warnings = _signals_from_corpus_or_db(
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        round_scope=normalized_round_scope,
        corpus_path=corpus_path,
        limit=limit,
    )
    coverage_payload, coverage_warnings = _load_approved_helper_artifact(
        coverage_audit_path,
        allowed_skills={"audit-public-discourse-sample-coverage"},
    )
    aggregation_payload, aggregation_warnings = _load_approved_helper_artifact(
        aggregation_path,
        allowed_skills={"aggregate-public-discourse-annotations"},
    )
    comparison_payload, comparison_warnings = _load_approved_helper_artifact(
        comparison_path,
        allowed_skills={"compare-public-media-narratives"},
    )
    metadata = _public_discourse_metadata(skill_name)
    warnings = [
        *corpus_warnings,
        *coverage_warnings,
        *aggregation_warnings,
        *comparison_warnings,
        *_artifact_warnings(corpus_payload, coverage_payload, aggregation_payload, comparison_payload),
    ]
    if not corpus_payload:
        warnings.append(
            {
                "code": "corpus-artifact-not-supplied",
                "message": "No approved corpus artifact was supplied; summary uses current DB-visible public/formal signals.",
            }
        )
    if not aggregation_payload:
        warnings.append(
            {
                "code": "annotation-aggregation-not-supplied",
                "message": "No approved annotation aggregation was supplied; issue, affect, and source narrative distributions may be empty.",
            }
        )
    if not comparison_payload:
        warnings.append(
            {
                "code": "comparison-artifact-not-supplied",
                "message": "No approved public/media narrative comparison was supplied; cross-source comparison is limited.",
            }
        )
    summary_id = "public-discourse-sample-summary-" + stable_hash(
        run_id,
        round_id,
        normalized_round_scope,
        corpus_path,
        coverage_audit_path,
        aggregation_path,
        comparison_path,
        len(signals),
    )[:12]
    sample_definition = dict_items(corpus_payload.get("sample_definition")) if corpus_payload else {
        "run_id": run_id,
        "round_id": round_id,
        "round_scope": normalized_round_scope,
        "sample_boundary": "DB-visible normalized public/formal text sample only",
    }
    gdelt_media_tone_summary = _distribution_from_payload(comparison_payload, "gdelt_media_tone_summary") or _numeric_metric_summary(signals)
    cross_source_comparison = dict_items(comparison_payload.get("cross_source_comparison")) if comparison_payload else {
        "comparison_scope": "not-supplied",
        "can_support": [],
        "cannot_support": [
            "general public opinion estimates",
            "physical source attribution",
            "report-ready conclusions",
        ],
    }
    examples = _sample_examples(
        signals=signals,
        corpus_payload=corpus_payload,
        limit=example_limit,
    )
    board_handoff = _summary_board_handoff(
        output_file=output_file,
        summary_id=summary_id,
        warnings=warnings,
    )
    payload = {
        "schema_version": "optional-analysis-public-discourse-sample-summary-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "helper_governance": metadata,
        "summary_id": summary_id,
        "sample_definition": sample_definition,
        "sample_count": int(corpus_payload.get("sample_count") or len(signals)) if corpus_payload else len(signals),
        "source_family_counts": list_items(corpus_payload.get("source_family_counts")) if corpus_payload else _source_family_counts(signals),
        "source_skill_counts": list_items(corpus_payload.get("source_skill_counts")) if corpus_payload else _source_skill_counts(signals),
        "discourse_lane_counts": list_items(corpus_payload.get("discourse_lane_counts")) if corpus_payload else _lane_counts(signals),
        "coverage_audit_summary": {
            "coverage_audit_id": maybe_text(coverage_payload.get("coverage_audit_id")),
            "coverage_cue_count": len(list_items(coverage_payload.get("coverage_cues"))),
            "source_family_counts": list_items(coverage_payload.get("source_family_counts")),
        },
        "source_acquisition_handoff": dict_items(
            coverage_payload.get("source_acquisition_handoff")
        ),
        "issue_distribution": _distribution_from_payload(aggregation_payload, "issue_distribution"),
        "social_affect_distribution": _distribution_from_payload(aggregation_payload, "social_affect_distribution"),
        "gdelt_media_tone_summary": gdelt_media_tone_summary,
        "source_narrative_distribution": _distribution_from_payload(aggregation_payload, "source_narrative_distribution"),
        "actor_responsibility_distribution": _distribution_from_payload(aggregation_payload, "actor_responsibility_distribution"),
        "action_orientation_distribution": _distribution_from_payload(aggregation_payload, "action_orientation_distribution"),
        "distribution_use_policy": _distribution_use_policy(),
        "cross_source_comparison": cross_source_comparison,
        "source_narrative_cross_lane_cues": _distribution_from_payload(comparison_payload, "source_narrative_cross_lane_cues"),
        "example_refs": examples,
        "evidence_refs": refs_from_signals(signals),
        "representativeness_limits": _artifact_representativeness_limits(
            corpus_payload,
            coverage_payload,
            aggregation_payload,
            comparison_payload,
        ),
        "board_handoff": board_handoff,
        "observed_inputs": {
            "db_path": db_path,
            "round_scope": normalized_round_scope,
            "corpus_path": maybe_text(corpus_path),
            "coverage_audit_path": maybe_text(coverage_audit_path),
            "aggregation_path": maybe_text(aggregation_path),
            "comparison_path": maybe_text(comparison_path),
        },
        "source_parameters": {"db_path": db_path},
        "query_parameters": {
            "run_id": run_id,
            "round_id": round_id,
            "round_scope": normalized_round_scope,
            "corpus_path": maybe_text(corpus_path),
            "coverage_audit_path": maybe_text(coverage_audit_path),
            "aggregation_path": maybe_text(aggregation_path),
            "comparison_path": maybe_text(comparison_path),
        },
        "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"], "db_path": db_path},
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
            "sample_count": payload["sample_count"],
            "example_count": len(examples),
            "decision_source": metadata["decision_source"],
            "rule_id": metadata["rule_id"],
        },
        "receipt_id": "public-discourse-summary-receipt-"
        + stable_hash(skill_name, run_id, round_id, output_file, summary_id)[:20],
        "batch_id": "public-discourse-summary-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$")],
        "canonical_ids": [summary_id],
        "warnings": warnings,
        "summary_id": summary_id,
        "board_handoff": board_handoff,
    }


def _corpus_warnings(signals: list[dict[str, Any]]) -> list[dict[str, str]]:
    warnings: list[dict[str, str]] = []
    if not signals:
        warnings.append(
            {
                "code": "no-corpus-items",
                "message": "No public/formal text-like signals matched the supplied corpus filters.",
            }
        )
    if signals and not any(public_discourse_lane(signal) == "social_sample_affect" for signal in signals):
        warnings.append(
            {
                "code": "no-social-sample-affect-basis",
                "message": "The matched sample has no YouTube comments or Bluesky posts; do not describe public affect from this corpus.",
            }
        )
    if any(public_discourse_lane(signal) in GDELT_TONE_LANES for signal in signals):
        warnings.append(
            {
                "code": "gdelt-tone-boundary",
                "message": "GDELT DOC/row tone may support media/document tone cues, not public sentiment proportions.",
            }
        )
    return warnings


def run_materialize_public_discourse_corpus(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    round_scope: str = "current",
    output_path: str = "",
    source_family: str = "",
    source_skill: str = "",
    keyword_any: list[str] | None = None,
    observed_after_utc: str = "",
    observed_before_utc: str = "",
    limit: int = 500,
) -> dict[str, Any]:
    skill_name = "materialize-public-discourse-corpus"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"public_discourse_corpus_{round_id}.json")
    normalized_round_scope = _normalized_round_scope(round_scope)
    source_signals, db_path = _load_discourse_signals(
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        round_scope=normalized_round_scope,
        limit=limit,
    )
    keywords = unique_texts(keyword_any or [])
    matched_signals = _filter_discourse_signals(
        source_signals,
        source_family=source_family,
        source_skill=source_skill,
        keyword_any=keywords,
        observed_after_utc=observed_after_utc,
        observed_before_utc=observed_before_utc,
    )[: max(1, int(limit or 500))]
    items = _corpus_items(matched_signals, run_id=run_id, round_id=round_id)
    corpus_id = "public-discourse-corpus-" + stable_hash(
        run_id,
        round_id,
        normalized_round_scope,
        source_family,
        source_skill,
        keywords,
        observed_after_utc,
        observed_before_utc,
        len(items),
    )[:12]
    metadata = _public_discourse_metadata(skill_name)
    warnings = _corpus_warnings(matched_signals)
    sample_definition = {
        "run_id": run_id,
        "round_id": round_id,
        "round_scope": normalized_round_scope,
        "source_family": maybe_text(source_family),
        "source_skill": maybe_text(source_skill),
        "keyword_any": keywords,
        "observed_after_utc": maybe_text(observed_after_utc),
        "observed_before_utc": maybe_text(observed_before_utc),
        "sample_boundary": "DB-visible normalized public/formal text sample only",
    }
    payload = {
        "schema_version": "optional-analysis-public-discourse-corpus-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "helper_governance": metadata,
        "corpus_id": corpus_id,
        "sample_definition": sample_definition,
        "sample_count": len(items),
        "source_family_counts": _source_family_counts(matched_signals),
        "source_skill_counts": _source_skill_counts(matched_signals),
        "discourse_lane_counts": _lane_counts(matched_signals),
        "corpus_items": items,
        "representativeness_limits": [
            "The corpus is not a representative public-opinion sample.",
            "Platform, query, time-window, API availability, and normalization choices bound all sample statements.",
            "Absence from this corpus is not evidence that a public concern or narrative is absent.",
        ],
        "observed_inputs": {
            "db_path": db_path,
            "round_scope": normalized_round_scope,
            "available_signal_count": len(source_signals),
            "matched_signal_count": len(matched_signals),
        },
        "source_parameters": {"db_path": db_path},
        "query_parameters": sample_definition,
        "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"], "db_path": db_path},
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
            "sample_count": len(items),
            "decision_source": metadata["decision_source"],
            "rule_id": metadata["rule_id"],
        },
        "receipt_id": "public-discourse-corpus-receipt-"
        + stable_hash(skill_name, run_id, round_id, output_file, corpus_id)[:20],
        "batch_id": "public-discourse-corpus-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.corpus_items")],
        "canonical_ids": [corpus_id],
        "warnings": warnings,
        "corpus_id": corpus_id,
        "sample_count": len(items),
        "board_handoff": safe_board_handoff(
            artifact_path=output_file,
            locator="$.corpus_items",
            candidate_ids=[corpus_id],
            gap_hints=[warning["message"] for warning in warnings],
        ),
    }


def _corpus_input_observation(corpus_path: str) -> tuple[dict[str, Any], list[dict[str, str]]]:
    if not maybe_text(corpus_path):
        return {}, []
    payload = load_json_file(corpus_path, {})
    approved, reason = approved_helper_input_payload(
        payload,
        allowed_skills={"materialize-public-discourse-corpus"},
    )
    if not approved:
        return {}, [unapproved_input_warning(corpus_path, reason)]
    return payload, []


def _family_coverage(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_skill = Counter(maybe_text(signal.get("source_skill")) for signal in signals)
    rows: list[dict[str, Any]] = []
    for family in PUBLIC_DISCOURSE_EXPECTED_FAMILIES:
        source_family = maybe_text(family.get("source_family"))
        skills = [
            *list_items(family.get("text_sample_skills")),
            *list_items(family.get("recon_skills")),
        ]
        skill_counts = [
            {"source_skill": skill, "signal_count": by_skill.get(skill, 0)}
            for skill in unique_texts(skills)
        ]
        rows.append(
            {
                "source_family": source_family,
                "observed_signal_count": sum(item["signal_count"] for item in skill_counts),
                "source_skill_counts": skill_counts,
                "coverage_status": "observed" if any(item["signal_count"] for item in skill_counts) else "not-observed-in-db-sample",
                "audit_status": "requires-human-review",
            }
        )
    return rows


def _coverage_warnings(signals: list[dict[str, Any]]) -> list[dict[str, str]]:
    by_skill = Counter(maybe_text(signal.get("source_skill")) for signal in signals)
    warnings: list[dict[str, str]] = []
    if not signals:
        warnings.append(
            {
                "code": "no-public-discourse-sample",
                "message": "No public or formal DB signals are visible for this round.",
            }
        )
        return warnings
    if by_skill.get("fetch-youtube-video-search", 0) and not by_skill.get("fetch-youtube-comments", 0):
        warnings.append(
            {
                "code": "youtube-comments-not-materialized",
                "message": "YouTube video candidates exist, but no YouTube comments are normalized; public-response affect is not supported from YouTube yet.",
            }
        )
    has_doc_rows = by_skill.get("fetch-gdelt-doc-search", 0)
    has_doc_tone = any(public_discourse_lane(signal) == "gdelt_doc_tone_aggregate" for signal in signals)
    has_row_tone = any(
        by_skill.get(skill_name, 0)
        for skill_name in ("fetch-gdelt-events", "fetch-gdelt-mentions", "fetch-gdelt-gkg")
    )
    if has_doc_rows and not has_doc_tone and not has_row_tone:
        warnings.append(
            {
                "code": "gdelt-row-layer-not-materialized",
                "message": "GDELT DOC recon rows exist, but neither DOC tone aggregates nor Events/Mentions/GKG row layers are materialized; media/document tone coverage is incomplete.",
            }
        )
    if not any(by_skill.get(skill_name, 0) for skill_name in SOCIAL_SAMPLE_AFFECT_SKILLS):
        warnings.append(
            {
                "code": "no-social-sample-affect-basis",
                "message": "No YouTube comments or Bluesky posts are normalized; do not infer social sample affect.",
            }
        )
    return warnings


def _public_discourse_missing_layer_handoff(
    warnings: list[dict[str, str]],
) -> dict[str, Any]:
    warning_codes = {
        maybe_text(warning.get("code"))
        for warning in warnings
        if isinstance(warning, dict)
    }
    missing_layers: list[dict[str, Any]] = []
    if "youtube-comments-not-materialized" in warning_codes:
        missing_layers.append(
            {
                "layer_id": "youtube-comments",
                "gap_code": "youtube-comments-not-materialized",
                "missing_object_kind": "normalized-public-comment-signal",
                "blocked_discourse_lane": "social_sample_affect",
                "prerequisite_source_skills": ["fetch-youtube-video-search"],
                "candidate_fetch_skills": ["fetch-youtube-comments"],
                "candidate_normalize_skills": ["normalize-youtube-comments-public-signals"],
                "required_configuration": ["YOUTUBE_API_KEY"],
                "council_handoff": (
                    "Submit or approve a source-acquisition proposal for YouTube comments "
                    "against selected video IDs before treating YouTube as a public-response "
                    "affect sample."
                ),
            }
        )
    if "gdelt-row-layer-not-materialized" in warning_codes:
        missing_layers.append(
            {
                "layer_id": "gdelt-doc-tone-or-events-mentions-gkg",
                "gap_code": "gdelt-row-layer-not-materialized",
                "missing_object_kind": "normalized-gdelt-tone-signal",
                "blocked_discourse_lane": "gdelt_doc_tone_aggregate_or_gdelt_media_tone",
                "prerequisite_source_skills": ["fetch-gdelt-doc-search"],
                "candidate_fetch_skills": [
                    "fetch-gdelt-doc-search",
                    "fetch-gdelt-events",
                    "fetch-gdelt-mentions",
                    "fetch-gdelt-gkg",
                ],
                "candidate_normalize_skills": [
                    "normalize-gdelt-doc-public-signals",
                    "normalize-gdelt-events-public-signals",
                    "normalize-gdelt-mentions-public-signals",
                    "normalize-gdelt-gkg-public-signals",
                ],
                "required_configuration": [],
                "council_handoff": (
                    "Submit or approve source-acquisition proposals for DOC timelinetone/tonechart "
                    "or GDELT Events/Mentions/GKG row layers before treating GDELT tone coverage as complete."
                ),
            }
        )
    if "no-social-sample-affect-basis" in warning_codes:
        missing_layers.append(
            {
                "layer_id": "social-sample-affect-basis",
                "gap_code": "no-social-sample-affect-basis",
                "missing_object_kind": "normalized-social-text-signal",
                "blocked_discourse_lane": "social_sample_affect",
                "prerequisite_source_skills": [],
                "candidate_fetch_skills": [
                    "fetch-youtube-comments",
                    "fetch-bluesky-cascade",
                ],
                "candidate_normalize_skills": [
                    "normalize-youtube-comments-public-signals",
                    "normalize-bluesky-cascade-public-signals",
                ],
                "required_configuration": ["YOUTUBE_API_KEY for YouTube comments"],
                "council_handoff": (
                    "Acquire at least one normalized social text sample before writing "
                    "sample affect distributions."
                ),
            }
        )
    suggested_next_skills = ["submit-source-acquisition-proposal"]
    if missing_layers:
        suggested_next_skills.extend(
            [
                "submit-evidence-request",
                "materialize-public-discourse-corpus",
                "audit-public-discourse-sample-coverage",
                "submit-readiness-opinion",
                "submit-round-synthesis",
            ]
        )
    return {
        "missing_layer_count": len(missing_layers),
        "missing_layers": missing_layers,
        "candidate_fetch_skills": unique_texts(
            [
                skill
                for layer in missing_layers
                for skill in list_items(layer.get("candidate_fetch_skills"))
            ]
        ),
        "candidate_normalize_skills": unique_texts(
            [
                skill
                for layer in missing_layers
                for skill in list_items(layer.get("candidate_normalize_skills"))
            ]
        ),
        "suggested_next_skills": unique_texts(suggested_next_skills)
        if missing_layers
        else [],
    }


def run_audit_public_discourse_sample_coverage(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    round_scope: str = "current",
    corpus_path: str = "",
    output_path: str = "",
    limit: int = 500,
) -> dict[str, Any]:
    skill_name = "audit-public-discourse-sample-coverage"
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(run_dir_path, output_path, f"public_discourse_coverage_audit_{round_id}.json")
    normalized_round_scope = _normalized_round_scope(round_scope)
    source_signals, db_path = _load_discourse_signals(
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        round_scope=normalized_round_scope,
        limit=limit,
    )
    corpus_payload, corpus_warnings = _corpus_input_observation(corpus_path)
    metadata = _public_discourse_metadata(skill_name)
    warnings = [*corpus_warnings, *_coverage_warnings(source_signals)]
    source_acquisition_handoff = _public_discourse_missing_layer_handoff(warnings)
    audit_id = "public-discourse-coverage-audit-" + stable_hash(
        run_id,
        round_id,
        normalized_round_scope,
        corpus_path,
        len(source_signals),
    )[:12]
    coverage_cues = [
        {
            "cue_id": "public-discourse-coverage-cue-" + stable_hash(run_id, round_id, item["source_family"])[:12],
            "cue_kind": "source-family-sample-coverage",
            **item,
            "evidence_refs": refs_from_signals(
                [
                    signal
                    for signal in source_signals
                    if public_discourse_source_family(signal) == item["source_family"]
                ]
            ),
            "lineage": lineage_from_signals(
                [
                    signal
                    for signal in source_signals
                    if public_discourse_source_family(signal) == item["source_family"]
                ]
            ),
            "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"], "db_path": db_path},
        }
        for item in _family_coverage(source_signals)
    ]
    payload = {
        "schema_version": "optional-analysis-public-discourse-coverage-audit-v1",
        "skill": skill_name,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "helper_governance": metadata,
        "coverage_audit_id": audit_id,
        "sample_count": len(source_signals),
        "source_family_counts": _source_family_counts(source_signals),
        "source_skill_counts": _source_skill_counts(source_signals),
        "discourse_lane_counts": _lane_counts(source_signals),
        "coverage_cues": coverage_cues,
        "source_acquisition_handoff": source_acquisition_handoff,
        "representativeness_limits": [
            "Coverage cues are prompts for council review, not findings of representativeness.",
            "Zero rows for a source family may reflect unrun fetches, filters, API limits, import scope, or normalization gaps.",
            "GDELT media/document tone, social sample affect, and physical source attribution must remain separate.",
        ],
        "observed_inputs": {
            "db_path": db_path,
            "round_scope": normalized_round_scope,
            "corpus_path": maybe_text(corpus_path),
            "corpus_item_count": len(list_items(corpus_payload.get("corpus_items"))) if corpus_payload else 0,
        },
        "source_parameters": {"db_path": db_path},
        "query_parameters": {"run_id": run_id, "round_id": round_id, "round_scope": normalized_round_scope},
        "provenance": {"source_skill": skill_name, "decision_source": metadata["decision_source"], "db_path": db_path},
        "warnings": warnings,
    }
    write_json(output_file, payload)
    board_handoff = safe_board_handoff(
        artifact_path=output_file,
        locator="$.coverage_cues",
        candidate_ids=[audit_id],
        gap_hints=[warning["message"] for warning in warnings],
    )
    board_handoff["suggested_next_skills"] = unique_texts(
        list_items(source_acquisition_handoff.get("suggested_next_skills"))
    )
    return {
        "status": "completed",
        "summary": {
            "skill": skill_name,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "coverage_cue_count": len(coverage_cues),
            "warning_count": len(warnings),
            "decision_source": metadata["decision_source"],
            "rule_id": metadata["rule_id"],
        },
        "receipt_id": "public-discourse-coverage-receipt-"
        + stable_hash(skill_name, run_id, round_id, output_file, audit_id)[:20],
        "batch_id": "public-discourse-coverage-batch-" + stable_hash(skill_name, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.coverage_cues")],
        "canonical_ids": [audit_id],
        "warnings": warnings,
        "coverage_audit_id": audit_id,
        "source_acquisition_handoff": source_acquisition_handoff,
        "board_handoff": board_handoff,
    }
