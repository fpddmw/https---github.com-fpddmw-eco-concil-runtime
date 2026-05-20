from __future__ import annotations

import json
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.core.manifest import load_json_if_exists
from eco_council_runtime.kernel.governance.round_liveness import (
    build_round_liveness_surface,
)
from eco_council_runtime.kernel.governance.skill_registry import resolve_skill_policy
from eco_council_runtime.kernel.planes.analysis_plane import (
    ANALYSIS_KIND_CLAIM_GAP_ACTION_CARD,
    query_analysis_result_sets,
    sync_analysis_result_set,
)
from eco_council_runtime.objects.council import query_council_objects

from .support import (
    artifact_ref,
    helper_metadata,
    list_items,
    maybe_text,
    pretty_json,
    resolve_output_path,
    resolve_run_dir,
    stable_hash,
    unique_texts,
    utc_now_iso,
    write_json,
)


SKILL_NAME = "materialize-claim-gap-action-cards"
ACTION_CARD_SCHEMA_VERSION = "optional-analysis-claim-gap-action-cards-v1"

PUBLIC_SEMANTIC_TERMS = (
    "public sentiment",
    "public opinion",
    "public response",
    "public reaction",
    "public concern",
    "sample affect",
    "semantic",
    "sentiment",
    "opinion",
    "emotion",
    "concern",
    "narrative",
    "公众",
    "舆情",
    "情绪",
    "民意",
    "语义",
    "关切",
    "叙事",
)
FORMAL_COMMENT_TERMS = (
    "formal comment",
    "public comment",
    "comment issue",
    "regulations.gov",
    "docket",
    "正式评论",
    "公众评论",
    "法规评论",
    "争点",
)
ENVIRONMENT_AGGREGATE_TERMS = (
    "trend",
    "peak",
    "range",
    "operating status",
    "environment trend",
    "环境趋势",
    "峰值",
    "运行状态",
    "水位",
    "pm2.5",
    "aqi",
)
INTERACTION_TERMS = (
    "interaction",
    "semantic shift",
    "timeline",
    "communication gap",
    "misalignment",
    "互动",
    "语义变化",
    "时间线",
    "沟通缺口",
    "错位",
)

PUBLIC_TEXT_SOURCE_SKILLS = {
    "fetch-youtube-comments",
    "fetch-bluesky-cascade",
    "normalize-youtube-comments-public-signals",
    "normalize-bluesky-cascade-public-signals",
}
GDELT_SOURCE_SKILLS = {
    "fetch-gdelt-doc-search",
    "fetch-gdelt-events",
    "fetch-gdelt-mentions",
    "fetch-gdelt-gkg",
    "normalize-gdelt-doc-public-signals",
    "normalize-gdelt-events-public-signals",
    "normalize-gdelt-mentions-public-signals",
    "normalize-gdelt-gkg-public-signals",
}
FORMAL_SOURCE_SKILLS = {
    "fetch-regulationsgov-comments",
    "fetch-regulationsgov-comment-detail",
    "fetch-regulationsgov-attachments",
    "normalize-regulationsgov-comments-public-signals",
    "normalize-regulationsgov-comment-detail-public-signals",
    "normalize-regulationsgov-attachment-text",
}
ENVIRONMENT_SOURCE_SKILLS = {
    "fetch-airnow-hourly-observations",
    "fetch-openaq",
    "fetch-open-meteo-air-quality",
    "fetch-open-meteo-historical",
    "fetch-open-meteo-flood",
    "fetch-usbr-rise",
    "fetch-usgs-water-iv",
    "fetch-nasa-firms-fire",
    "normalize-airnow-observation-signals",
    "normalize-openaq-observation-signals",
    "normalize-open-meteo-air-quality-signals",
    "normalize-open-meteo-historical-signals",
    "normalize-open-meteo-flood-signals",
    "normalize-usbr-rise-environment-signals",
    "normalize-usgs-water-observation-signals",
    "normalize-nasa-firms-fire-observation-signals",
}
NONPRODUCTIVE_ATTEMPT_STATUSES = {"failed", "blocked", "receipt-only"}
EXECUTED_ATTEMPT_STATUSES = {"executed", "fetched", "normalized"}


def _contains_any(text: str, terms: tuple[str, ...]) -> bool:
    haystack = maybe_text(text).casefold()
    return any(term.casefold() in haystack for term in terms)


def _safe_json(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    text = maybe_text(value)
    if not text:
        return default
    try:
        payload = json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError):
        return default
    return payload if isinstance(payload, type(default)) else default


def _mission_focus(run_dir: Path, round_id: str) -> str:
    mission_payload = load_json_if_exists(run_dir / "mission.json") or {}
    scaffold_payload = load_json_if_exists(run_dir / "runtime" / f"mission_scaffold_{round_id}.json") or {}
    pieces = [
        mission_payload.get("request_text"),
        mission_payload.get("objective"),
        mission_payload.get("topic"),
        scaffold_payload.get("request_text"),
        scaffold_payload.get("objective"),
        scaffold_payload.get("topic"),
    ]
    return maybe_text(" ".join(maybe_text(piece) for piece in pieces if maybe_text(piece)))


def _query_objects(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    object_kind: str,
    limit: int = 200,
) -> list[dict[str, Any]]:
    try:
        payload = query_council_objects(
            run_dir,
            object_kind=object_kind,
            run_id=run_id,
            round_id=round_id,
            limit=limit,
        )
    except Exception:
        return []
    return [
        item
        for item in list_items(payload.get("objects"))
        if isinstance(item, dict)
    ]


def _all_council_objects(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
) -> dict[str, list[dict[str, Any]]]:
    kinds = [
        "evidence-request",
        "source-acquisition-proposal",
        "evidence-route-assessment",
        "finding",
        "evidence-bundle",
        "readiness-opinion",
        "challenge",
        "review-comment",
        "round-synthesis",
        "agent-position",
    ]
    return {
        kind: _query_objects(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            object_kind=kind,
        )
        for kind in kinds
    }


def _signal_counts(run_dir: Path, *, run_id: str, round_id: str) -> dict[str, Any]:
    db_path = (run_dir / "analytics" / "signal_plane.sqlite").resolve()
    if not db_path.exists():
        return {
            "db_path": str(db_path),
            "present": False,
            "total_count": 0,
            "plane_counts": {},
            "source_skill_counts": {},
            "public_text_sample_count": 0,
            "gdelt_tone_count": 0,
            "gdelt_record_count": 0,
            "formal_text_count": 0,
            "environment_count": 0,
        }
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        table_present = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'normalized_signals'"
        ).fetchone()
        if table_present is None:
            return {
                "db_path": str(db_path),
                "present": True,
                "total_count": 0,
                "plane_counts": {},
                "source_skill_counts": {},
                "public_text_sample_count": 0,
                "gdelt_tone_count": 0,
                "gdelt_record_count": 0,
                "formal_text_count": 0,
                "environment_count": 0,
            }
        rows = connection.execute(
            """
            SELECT plane, source_skill, signal_kind, metric, metadata_json
            FROM normalized_signals
            WHERE run_id = ? AND round_id = ?
            """,
            (run_id, round_id),
        ).fetchall()
    finally:
        connection.close()

    plane_counts: Counter[str] = Counter()
    source_skill_counts: Counter[str] = Counter()
    public_text_sample_count = 0
    gdelt_tone_count = 0
    gdelt_record_count = 0
    formal_text_count = 0
    environment_count = 0
    for row in rows:
        plane = maybe_text(row["plane"])
        source_skill = maybe_text(row["source_skill"])
        signal_kind = maybe_text(row["signal_kind"]).casefold()
        metric = maybe_text(row["metric"]).casefold()
        metadata = _safe_json(row["metadata_json"], {})
        metadata_text = json.dumps(metadata, ensure_ascii=True, sort_keys=True).casefold()
        if plane:
            plane_counts[plane] += 1
        if source_skill:
            source_skill_counts[source_skill] += 1
        if source_skill in PUBLIC_TEXT_SOURCE_SKILLS:
            public_text_sample_count += 1
        if source_skill in GDELT_SOURCE_SKILLS:
            gdelt_record_count += 1
        if "tone" in metric or "tone" in signal_kind or "tone" in metadata_text:
            if source_skill in GDELT_SOURCE_SKILLS or "gdelt" in source_skill:
                gdelt_tone_count += 1
        if source_skill in FORMAL_SOURCE_SKILLS or plane == "formal":
            formal_text_count += 1
        if source_skill in ENVIRONMENT_SOURCE_SKILLS or plane == "environment":
            environment_count += 1
    return {
        "db_path": str(db_path),
        "present": True,
        "total_count": len(rows),
        "plane_counts": dict(plane_counts),
        "source_skill_counts": dict(source_skill_counts),
        "public_text_sample_count": public_text_sample_count,
        "gdelt_tone_count": gdelt_tone_count,
        "gdelt_record_count": gdelt_record_count,
        "formal_text_count": formal_text_count,
        "environment_count": environment_count,
    }


def _artifact_payload(path: Path) -> dict[str, Any]:
    payload = load_json_if_exists(path)
    return payload if isinstance(payload, dict) else {}


def _helper_artifacts(run_dir: Path, round_id: str) -> dict[str, dict[str, Any]]:
    analytics = run_dir / "analytics"
    known = {
        "public_discourse_corpus": analytics / f"public_discourse_corpus_{round_id}.json",
        "public_discourse_coverage_audit": analytics / f"public_discourse_coverage_audit_{round_id}.json",
        "public_discourse_annotation_aggregation": analytics / f"public_discourse_annotation_aggregation_{round_id}.json",
        "public_media_narrative_comparison": analytics / f"public_media_narrative_comparison_{round_id}.json",
        "public_discourse_sample_summary": analytics / f"public_discourse_sample_summary_{round_id}.json",
        "formal_comment_candidate_corpus_audit": analytics / f"formal_comment_candidate_corpus_audit_{round_id}.json",
        "formal_comment_issue_annotations": analytics / f"formal_comment_issue_annotations_{round_id}.json",
        "environment_evidence_aggregate": analytics / f"environment_evidence_aggregate_{round_id}.json",
        "spatiotemporal_relation_cues": analytics / f"spatiotemporal_relation_cues_{round_id}.json",
        "reporting_handoff": run_dir / "reporting" / f"reporting_handoff_{round_id}.json",
        "round_readiness": run_dir / "reporting" / f"round_readiness_{round_id}.json",
    }
    results: dict[str, dict[str, Any]] = {}
    for key, path in known.items():
        payload = _artifact_payload(path)
        results[key] = {
            "present": bool(payload),
            "path": str(path.resolve()),
            "skill": maybe_text(payload.get("skill")),
            "status": maybe_text(payload.get("status")),
            "summary": {
                field_name: payload.get(field_name)
                for field_name in (
                    "sample_count",
                    "annotation_count",
                    "coverage_audit_id",
                    "aggregation_id",
                    "aggregate_id",
                    "handoff_status",
                    "readiness_status",
                )
                if field_name in payload
            },
        }
    return results


def _analysis_summary(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
) -> dict[str, Any]:
    try:
        payload = query_analysis_result_sets(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            latest_only=True,
            limit=200,
        )
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "failed",
            "available_analysis_kinds": [],
            "warnings": [
                {
                    "code": "analysis-result-query-failed",
                    "message": str(exc),
                }
            ],
        }
    rows = [
        item
        for item in list_items(payload.get("result_sets"))
        if isinstance(item, dict)
    ]
    return {
        "status": "completed",
        "available_analysis_kinds": unique_texts(
            [row.get("analysis_kind") for row in rows]
        ),
        "result_set_count": len(rows),
        "result_sets": [
            {
                "analysis_kind": maybe_text(row.get("analysis_kind")),
                "source_skill": maybe_text(row.get("source_skill")),
                "item_count": int(row.get("item_count") or 0),
                "artifact_path": maybe_text(row.get("artifact_path")),
            }
            for row in rows
        ],
        "warnings": list_items(payload.get("warnings")),
    }


def _object_ref(kind: str, payload: dict[str, Any]) -> str:
    for field_name in (
        "object_id",
        "proposal_id",
        "request_id",
        "ticket_id",
        "comment_id",
        "opinion_id",
        "finding_id",
        "bundle_id",
        "synthesis_id",
        "position_id",
        "assessment_id",
    ):
        value = maybe_text(payload.get(field_name))
        if value:
            return f"{kind}:{value}"
    return ""


def _contract_followups(skill_name: str) -> list[str]:
    try:
        policy = resolve_skill_policy(skill_name)
    except Exception:
        return []
    contract = policy.get("skill_contract") if isinstance(policy.get("skill_contract"), dict) else {}
    return unique_texts(
        contract.get("followups", []) if isinstance(contract.get("followups"), list) else []
    )


def _candidate_recovery_skills(source_skill: str) -> list[str]:
    return unique_texts(
        [
            source_skill,
            *_contract_followups(source_skill),
            "submit-evidence-route-assessment",
            "submit-round-synthesis",
        ]
    )


def _new_card(
    cards: list[dict[str, Any]],
    *,
    run_id: str,
    round_id: str,
    card_kind: str,
    claim_gap: str,
    why_it_matters: str,
    candidate_skills: list[str],
    required_inputs: list[str],
    expected_artifacts: list[str],
    if_not_done_report_boundary: str,
    owner_role_suggestions: list[str],
    evidence_refs: list[str] | None = None,
    source_attempt_refs: list[str] | None = None,
    challenge_refs: list[str] | None = None,
    readiness_refs: list[str] | None = None,
    lineage: list[str] | None = None,
) -> None:
    card = {
        "card_id": "claim-gap-card-"
        + stable_hash(run_id, round_id, card_kind, claim_gap, len(cards))[:12],
        "card_kind": maybe_text(card_kind),
        "claim_gap": maybe_text(claim_gap),
        "why_it_matters": maybe_text(why_it_matters),
        "candidate_skills": unique_texts(candidate_skills),
        "required_inputs": unique_texts(required_inputs),
        "expected_artifacts": unique_texts(expected_artifacts),
        "if_not_done_report_boundary": maybe_text(if_not_done_report_boundary),
        "owner_role_suggestions": unique_texts(owner_role_suggestions),
        "evidence_refs": unique_texts(evidence_refs or []),
        "source_attempt_refs": unique_texts(source_attempt_refs or []),
        "challenge_refs": unique_texts(challenge_refs or []),
        "readiness_refs": unique_texts(readiness_refs or []),
        "lineage": unique_texts(lineage or []),
        "advisory_semantics": (
            "This action card is claim-basis advisory only. It does not rank, "
            "score, schedule, gate, or execute a skill."
        ),
    }
    cards.append(card)


def _public_semantic_basis_missing(
    helper_artifacts: dict[str, dict[str, Any]],
) -> dict[str, bool]:
    corpus_present = bool(helper_artifacts.get("public_discourse_corpus", {}).get("present"))
    coverage_present = bool(helper_artifacts.get("public_discourse_coverage_audit", {}).get("present"))
    aggregation = helper_artifacts.get("public_discourse_annotation_aggregation", {})
    aggregation_present = bool(aggregation.get("present"))
    aggregation_summary = aggregation.get("summary") if isinstance(aggregation.get("summary"), dict) else {}
    denominator_present = aggregation_present and "annotation_count" in aggregation_summary
    return {
        "corpus_missing": not corpus_present,
        "coverage_missing": not coverage_present,
        "aggregation_missing": not aggregation_present,
        "denominator_missing": not denominator_present,
    }


def _attempt_kind(
    proposal: dict[str, Any],
    *,
    signal_counts: dict[str, Any],
    low_volume_threshold: int,
) -> str:
    status = maybe_text(proposal.get("status"))
    source_skill = maybe_text(proposal.get("source_skill"))
    normalized_refs = unique_texts(
        list_items(proposal.get("normalized_signal_refs"))
    )
    if status in NONPRODUCTIVE_ATTEMPT_STATUSES:
        return status
    source_counts = signal_counts.get("source_skill_counts")
    source_count = (
        int(source_counts.get(source_skill) or 0)
        if isinstance(source_counts, dict)
        else 0
    )
    if status in EXECUTED_ATTEMPT_STATUSES and not normalized_refs and source_count == 0:
        return "zero-result"
    if (
        status in EXECUTED_ATTEMPT_STATUSES
        and 0 < source_count < max(2, int(low_volume_threshold or 3))
    ):
        return "low-volume"
    return ""


def run_materialize_claim_gap_action_cards(
    *,
    run_dir: str,
    run_id: str,
    round_id: str,
    output_path: str = "",
    low_volume_threshold: int = 3,
    max_cards: int = 50,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_output_path(
        run_dir_path,
        output_path,
        f"claim_gap_action_cards_{round_id}.json",
    )
    mission_focus = _mission_focus(run_dir_path, round_id)
    council_objects = _all_council_objects(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    signal_counts = _signal_counts(run_dir_path, run_id=run_id, round_id=round_id)
    helper_artifacts = _helper_artifacts(run_dir_path, round_id)
    analysis = _analysis_summary(run_dir_path, run_id=run_id, round_id=round_id)
    try:
        liveness = build_round_liveness_surface(
            run_dir_path,
            run_id=run_id,
            round_id=round_id,
        )
    except Exception as exc:  # noqa: BLE001
        liveness = {
            "status": "failed",
            "warnings": [
                {
                    "code": "round-liveness-query-failed",
                    "message": str(exc),
                }
            ],
        }

    combined_focus_text = " ".join(
        [
            mission_focus,
            *[
                maybe_text(item.get("question"))
                + " "
                + maybe_text(item.get("desired_evidence_type"))
                + " "
                + maybe_text(item.get("rationale"))
                for item in council_objects.get("evidence-request", [])
            ],
            *[
                maybe_text(item.get("rationale"))
                + " "
                + maybe_text(item.get("readiness_status"))
                for item in council_objects.get("readiness-opinion", [])
            ],
        ]
    )

    cards: list[dict[str, Any]] = []
    public_semantic_requested = _contains_any(combined_focus_text, PUBLIC_SEMANTIC_TERMS)
    formal_comment_requested = _contains_any(combined_focus_text, FORMAL_COMMENT_TERMS)
    environment_aggregate_requested = _contains_any(combined_focus_text, ENVIRONMENT_AGGREGATE_TERMS)
    interaction_requested = _contains_any(combined_focus_text, INTERACTION_TERMS)

    if public_semantic_requested:
        missing = _public_semantic_basis_missing(helper_artifacts)
        if any(missing.values()):
            missing_inputs = [
                label
                for label, is_missing in (
                    ("materialized public-policy corpus", missing["corpus_missing"]),
                    ("coverage audit", missing["coverage_missing"]),
                    ("annotation aggregation", missing["aggregation_missing"]),
                    ("explicit denominator", missing["denominator_missing"]),
                )
                if is_missing
            ]
            _new_card(
                cards,
                run_id=run_id,
                round_id=round_id,
                card_kind="claim-gap",
                claim_gap="Public semantic claim lacks corpus, coverage, annotation, aggregation, or denominator basis.",
                why_it_matters=(
                    "Public emotion, concern, narrative, or proportion language "
                    "needs sample definition and denominator visibility before it "
                    "can be written as a sample-local structure claim."
                ),
                candidate_skills=[
                    "materialize-public-discourse-corpus",
                    "audit-public-discourse-sample-coverage",
                    "classify-public-discourse-affect",
                    "aggregate-public-discourse-annotations",
                    "summarize-public-discourse-sample",
                ],
                required_inputs=[
                    "mission focus",
                    "source family",
                    "query variants",
                    "time window",
                    "normalized public/formal text signals",
                    *missing_inputs,
                ],
                expected_artifacts=[
                    "materialized_public_policy_corpus",
                    "public_policy_corpus_coverage_audit",
                    "public_semantic_annotations",
                    "semantic_aggregate",
                    "distribution_denominators",
                ],
                if_not_done_report_boundary=(
                    "Report may describe individual examples or small-sample cues "
                    "only; do not write public sentiment, main concern, or "
                    "percentage/proportion claims."
                ),
                owner_role_suggestions=["social-investigator", "moderator", "challenger"],
            )

    if formal_comment_requested and (
        not helper_artifacts.get("formal_comment_candidate_corpus_audit", {}).get("present")
        or not helper_artifacts.get("formal_comment_issue_annotations", {}).get("present")
    ):
        _new_card(
            cards,
            run_id=run_id,
            round_id=round_id,
            card_kind="claim-gap",
            claim_gap="Formal comment issue or stance structure lacks candidate audit and readable-text classification basis.",
            why_it_matters=(
                "Formal-comment structure claims need a candidate universe, "
                "readable detail or attachment text, and bounded issue labels."
            ),
            candidate_skills=[
                "audit-formal-comment-candidate-corpus",
                "fetch-regulationsgov-comment-detail",
                "fetch-regulationsgov-attachments",
                "normalize-regulationsgov-attachment-text",
                "classify-formal-comment-issues",
            ],
            required_inputs=[
                "docket or document identifiers",
                "candidate comment listing",
                "readable comment detail or attachment text",
                "sample definition",
            ],
            expected_artifacts=[
                "formal_comment_candidate_corpus_audit",
                "comment detail/text artifacts",
                "formal comment issue annotations",
            ],
            if_not_done_report_boundary=(
                "Report may say candidate comment entry points were found, but "
                "must not claim main formal issues, stance distribution, or "
                "participation structure."
            ),
            owner_role_suggestions=["social-investigator", "challenger"],
        )

    if environment_aggregate_requested and int(signal_counts.get("environment_count") or 0) and not helper_artifacts.get("environment_evidence_aggregate", {}).get("present"):
        _new_card(
            cards,
            run_id=run_id,
            round_id=round_id,
            card_kind="claim-gap",
            claim_gap="Environment trend, peak, range, or operating-status claim lacks aggregate environment evidence basis.",
            why_it_matters=(
                "Trend and operating-status wording needs a descriptive aggregate "
                "or must remain at item-level illustration."
            ),
            candidate_skills=["aggregate-environment-evidence"],
            required_inputs=[
                "normalized environment or operations signals",
                "metric/window/geography definition",
            ],
            expected_artifacts=[
                "environment_process_summary",
                "aggregate-environment-evidence artifact",
                "fact_claim_boundary",
            ],
            if_not_done_report_boundary=(
                "Report may cite specific observation examples only; do not write "
                "trend, peak, or operating-status conclusions."
            ),
            owner_role_suggestions=["environmental-investigator", "challenger"],
        )

    if interaction_requested and "fact-policy-public-interaction-timeline" not in analysis.get("available_analysis_kinds", []):
        _new_card(
            cards,
            run_id=run_id,
            round_id=round_id,
            card_kind="claim-gap",
            claim_gap="Fact, policy action, and public/media semantic interaction claim lacks an interaction timeline with two-sided refs.",
            why_it_matters=(
                "Interaction language needs fact/policy evidence and public/media "
                "semantic evidence on the same node; otherwise timing overlap can "
                "be mistaken for causation."
            ),
            candidate_skills=[
                "aggregate-environment-evidence",
                "compare-public-media-narratives",
                "review-spatiotemporal-relation-alternatives",
            ],
            required_inputs=[
                "fact or official action refs",
                "public/media semantic refs",
                "time-window alignment",
                "challenger limitation review",
            ],
            expected_artifacts=[
                "fact_policy_public_interaction_timeline",
                "semantic_shift_events",
                "communication_gap_notes",
                "misalignment_and_uncertainty_register",
            ],
            if_not_done_report_boundary=(
                "Report may present parallel fact/policy/public timelines or "
                "unverified cues only; do not write interaction or causal response claims."
            ),
            owner_role_suggestions=["moderator", "social-investigator", "environmental-investigator", "challenger"],
        )

    if public_semantic_requested and int(signal_counts.get("gdelt_tone_count") or 0):
        _new_card(
            cards,
            run_id=run_id,
            round_id=round_id,
            card_kind="source-limit",
            claim_gap="GDELT tone is visible but cannot serve as public sentiment basis.",
            why_it_matters=(
                "GDELT tone describes indexed media/document tone. Using it as "
                "public sentiment would mix source families and denominators."
            ),
            candidate_skills=[
                "compare-public-media-narratives",
                "materialize-public-discourse-corpus",
                "audit-public-discourse-sample-coverage",
            ],
            required_inputs=[
                "separate media/document tone sample",
                "separate public/social text sample if public sentiment is claimed",
                "source-family denominator separation",
            ],
            expected_artifacts=[
                "media_tone_summary",
                "source_limit_rationale",
                "coverage audit with GDELT/public denominator separation",
            ],
            if_not_done_report_boundary=(
                "Report may describe media/document tone only; do not describe "
                "public sentiment, public emotion, or public opinion from GDELT tone."
            ),
            owner_role_suggestions=["social-investigator", "challenger", "report-editor"],
        )

    for proposal in council_objects.get("source-acquisition-proposal", []):
        attempt_kind = _attempt_kind(
            proposal,
            signal_counts=signal_counts,
            low_volume_threshold=low_volume_threshold,
        )
        if not attempt_kind:
            continue
        source_skill = maybe_text(proposal.get("source_skill"))
        proposal_ref = _object_ref("source-acquisition-proposal", proposal)
        _new_card(
            cards,
            run_id=run_id,
            round_id=round_id,
            card_kind="recovery" if attempt_kind != "low-volume" else "source-limit",
            claim_gap=f"{attempt_kind} acquisition attempt for {source_skill or 'unknown source skill'} needs recovery choice or report boundary.",
            why_it_matters=(
                "A failed, zero, low-volume, or receipt-only acquisition attempt "
                "does not prove source absence; it only limits the current route."
            ),
            candidate_skills=_candidate_recovery_skills(source_skill),
            required_inputs=[
                "attempt parameters",
                "receipt or artifact refs",
                "status rationale",
                "untried same-family or alternate routes",
            ],
            expected_artifacts=[
                "revised source acquisition proposal",
                "evidence-route-assessment",
                "source-limit rationale",
                "updated council object or round synthesis",
            ],
            if_not_done_report_boundary=(
                "Report must state the acquisition/source limitation and must not "
                "treat the attempt result as evidence that real-world records or "
                "public concern are absent."
            ),
            owner_role_suggestions=unique_texts(
                [
                    maybe_text(proposal.get("author_role")),
                    "moderator",
                    "challenger",
                ]
            ),
            evidence_refs=unique_texts(list_items(proposal.get("evidence_refs"))),
            source_attempt_refs=[proposal_ref],
            lineage=[proposal_ref],
        )

    for challenge in council_objects.get("challenge", []):
        status = maybe_text(challenge.get("status"))
        if status in {"closed", "resolved", "completed"}:
            continue
        challenge_ref = _object_ref("challenge", challenge)
        _new_card(
            cards,
            run_id=run_id,
            round_id=round_id,
            card_kind="claim-gap",
            claim_gap="Open challenger constraint has not been resolved or carried as a report boundary.",
            why_it_matters=(
                "Open challenges are council-visible objections to claim scope, "
                "basis, attribution, representativeness, or report wording."
            ),
            candidate_skills=[
                "submit-challenge-disposition",
                "open-followup-from-review-comment",
                "submit-round-synthesis",
            ],
            required_inputs=[
                "challenge statement",
                "target object",
                "evidence refs or accepted limitation",
            ],
            expected_artifacts=[
                "challenge disposition",
                "follow-up evidence request",
                "round synthesis limitation",
            ],
            if_not_done_report_boundary=(
                "Report must not present the challenged claim as resolved; carry "
                "the objection as limitation or omit/downgrade the claim."
            ),
            owner_role_suggestions=["challenger", "moderator", "report-editor"],
            challenge_refs=[challenge_ref],
            lineage=[challenge_ref],
        )

    for opinion in council_objects.get("readiness-opinion", []):
        readiness = maybe_text(opinion.get("readiness_status"))
        if readiness in {"ready", "report-ready"} or bool(opinion.get("sufficient_for_report_basis")):
            continue
        opinion_ref = _object_ref("readiness-opinion", opinion)
        _new_card(
            cards,
            run_id=run_id,
            round_id=round_id,
            card_kind="report-readiness-gap",
            claim_gap="Readiness opinion records a report-basis gap or blocked posture.",
            why_it_matters=(
                "Readiness gaps identify claim boundaries the report must carry "
                "unless a later council object resolves them."
            ),
            candidate_skills=[
                "submit-evidence-request",
                "submit-evidence-route-assessment",
                "submit-readiness-opinion",
                "submit-round-synthesis",
            ],
            required_inputs=[
                "readiness rationale",
                "basis object ids",
                "unresolved refs",
            ],
            expected_artifacts=[
                "resolved readiness opinion",
                "accepted limitation",
                "follow-up round synthesis",
            ],
            if_not_done_report_boundary=(
                "Report may proceed only as a bounded report that names the "
                "readiness limitation; do not upgrade affected claims."
            ),
            owner_role_suggestions=[
                maybe_text(opinion.get("agent_role")) or "moderator",
                "moderator",
                "report-editor",
            ],
            readiness_refs=[opinion_ref],
            lineage=[opinion_ref],
        )

    cards = cards[: max(1, int(max_cards or 50))]
    metadata = helper_metadata(
        skill_name=SKILL_NAME,
        destination="claim-basis advisory action cards",
        caveats=[
            "Action cards are advisory only and cannot rank, score, schedule, or execute skills.",
            "Agents must adopt, reject, or rewrite useful cards through council objects before report-basis use.",
            "Zero, failed, low-volume, and receipt-only attempts are source limits or recovery prompts, not evidence absence.",
        ],
    )
    observed_input_summary = {
        "council_object_counts": {
            kind: len(items)
            for kind, items in council_objects.items()
        },
        "normalized_signal_counts": signal_counts,
        "helper_artifact_presence": {
            key: bool(value.get("present"))
            for key, value in helper_artifacts.items()
        },
        "analysis_result_set_count": int(analysis.get("result_set_count") or 0),
        "round_liveness_status": maybe_text(liveness.get("status")),
    }
    payload = {
        "schema_version": ACTION_CARD_SCHEMA_VERSION,
        "skill": SKILL_NAME,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "helper_governance": metadata,
        "mission_focus": mission_focus,
        "advisory_semantics": (
            "Action cards are emitted in discovery order. They have no priority, "
            "rank, score, weight, scheduler semantics, or automatic execution path."
        ),
        "action_card_count": len(cards),
        "action_cards": cards,
        "observed_input_summary": observed_input_summary,
        "source_parameters": {"db_path": signal_counts.get("db_path", "")},
        "query_parameters": {
            "run_id": run_id,
            "round_id": round_id,
            "low_volume_threshold": int(low_volume_threshold or 3),
            "max_cards": int(max_cards or 50),
        },
        "provenance": {
            "source_skill": SKILL_NAME,
            "decision_source": metadata["decision_source"],
        },
        "warnings": list_items(analysis.get("warnings")) + list_items(liveness.get("warnings")),
    }
    write_json(output_file, payload)
    analysis_sync = sync_analysis_result_set(
        run_dir_path,
        analysis_kind=ANALYSIS_KIND_CLAIM_GAP_ACTION_CARD,
        expected_run_id=run_id,
        round_id=round_id,
        artifact_path=output_file,
    )
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "action_card_count": len(cards),
            "decision_source": metadata["decision_source"],
            "rule_id": metadata["rule_id"],
        },
        "receipt_id": "claim-gap-action-cards-receipt-"
        + stable_hash(SKILL_NAME, run_id, round_id, output_file, len(cards))[:20],
        "batch_id": "claim-gap-action-cards-batch-" + stable_hash(SKILL_NAME, run_id, round_id)[:16],
        "artifact_refs": [artifact_ref(output_file, "$.action_cards")],
        "canonical_ids": [card["card_id"] for card in cards],
        "warnings": payload["warnings"],
        "analysis_sync": analysis_sync,
        "board_handoff": {
            "candidate_ids": [card["card_id"] for card in cards],
            "evidence_refs": [artifact_ref(output_file, "$.action_cards")],
            "gap_hints": [card["claim_gap"] for card in cards],
            "challenge_hints": [
                "Review action cards as advisory claim-basis prompts; do not treat them as required execution or source ranking."
            ],
            "suggested_next_skills": [],
        },
    }


__all__ = [
    "ACTION_CARD_SCHEMA_VERSION",
    "SKILL_NAME",
    "pretty_json",
    "run_materialize_claim_gap_action_cards",
]
