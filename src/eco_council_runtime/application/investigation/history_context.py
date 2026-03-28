"""History-retrieval snapshots and compact moderator context rendering."""

from __future__ import annotations

import argparse
import math
from pathlib import Path
from typing import Any

from eco_council_runtime.adapters.filesystem import load_json_if_exists, write_json, write_text
from eco_council_runtime.case_library import (
    command_search_cases,
    connect_db,
    load_case_bundle,
    normalized_values,
    parse_json_text,
    search_terms,
)
from eco_council_runtime.controller.constants import DEFAULT_HISTORY_TOP_K, MAX_HISTORY_TOP_K
from eco_council_runtime.controller.paths import (
    history_context_path,
    history_retrieval_path,
    investigation_plan_path,
    mission_path,
)
from eco_council_runtime.domain.text import maybe_text, truncate_text, unique_strings
from eco_council_runtime.investigation import build_investigation_plan

SCHEMA_VERSION = "1.0.0"
MAX_SURFACED_HISTORY_CASES = 3
MAX_EXCERPTS_PER_CASE = 2
MAX_EXCERPT_CHARS = 280
MATCH_TIER_PRIORITY = {"structured-strong": 3, "structured-weak": 2, "region": 1, "lexical": 0}
EXCERPT_KIND_PRIORITY = {
    "evidence-card": 0,
    "decision-summary": 1,
    "round-summary": 2,
    "report-summary": 3,
    "curated-summary": 4,
}
EXCERPT_KIND_BONUS = {
    "evidence-card": 2.3,
    "decision-summary": 1.8,
    "round-summary": 1.4,
    "report-summary": 1.1,
    "curated-summary": 0.9,
}


def normalize_history_candidate_limit(value: Any) -> int:
    try:
        count = int(value)
    except (TypeError, ValueError):
        count = DEFAULT_HISTORY_TOP_K
    return max(1, min(MAX_HISTORY_TOP_K, count))


def _estimated_tokens(*parts: Any) -> int:
    text = " ".join(maybe_text(part) for part in parts if maybe_text(part))
    if not text:
        return 0
    return max(1, int(math.ceil(len(text) / 4.0)))


def _history_plan_payload(run_dir: Path, round_id: str, mission_payload: dict[str, Any]) -> dict[str, Any]:
    plan_payload = load_json_if_exists(investigation_plan_path(run_dir, round_id))
    if not isinstance(plan_payload, dict) or not isinstance(plan_payload.get("history_query"), dict):
        plan_payload = build_investigation_plan(mission=mission_payload, round_id=round_id)
    return plan_payload


def _history_query_payload(mission_payload: dict[str, Any], plan_payload: dict[str, Any]) -> dict[str, Any]:
    history_query = plan_payload.get("history_query") if isinstance(plan_payload.get("history_query"), dict) else {}
    region = mission_payload.get("region") if isinstance(mission_payload.get("region"), dict) else {}
    return {
        "query": maybe_text(history_query.get("query")) or maybe_text(mission_payload.get("topic")),
        "region_label": maybe_text(history_query.get("region_label")) or maybe_text(region.get("label")),
        "profile_id": maybe_text(history_query.get("profile_id")) or maybe_text(plan_payload.get("profile_id")),
        "claim_types": [maybe_text(item) for item in history_query.get("claim_types", []) if maybe_text(item)],
        "metric_families": [maybe_text(item) for item in history_query.get("metric_families", []) if maybe_text(item)],
        "gap_types": [maybe_text(item) for item in history_query.get("gap_types", []) if maybe_text(item)],
        "source_skills": [maybe_text(item) for item in history_query.get("source_skills", []) if maybe_text(item)],
        "priority_leg_ids": [maybe_text(item) for item in history_query.get("priority_leg_ids", []) if maybe_text(item)],
        "alternative_hypotheses": [
            maybe_text(item) for item in history_query.get("alternative_hypotheses", []) if maybe_text(item)
        ],
    }


def _compact_plan_context(plan_payload: dict[str, Any]) -> dict[str, Any]:
    hypotheses = plan_payload.get("hypotheses") if isinstance(plan_payload.get("hypotheses"), list) else []
    compact_hypotheses: list[dict[str, Any]] = []
    for hypothesis in hypotheses[:3]:
        if not isinstance(hypothesis, dict):
            continue
        alternatives = hypothesis.get("alternative_hypotheses") if isinstance(hypothesis.get("alternative_hypotheses"), list) else []
        compact_hypotheses.append(
            {
                "hypothesis_id": maybe_text(hypothesis.get("hypothesis_id")),
                "summary": maybe_text(hypothesis.get("summary")) or maybe_text(hypothesis.get("statement")),
                "alternative_hypotheses": [
                    maybe_text(item.get("summary")) or maybe_text(item.get("statement"))
                    for item in alternatives[:2]
                    if isinstance(item, dict)
                    and (maybe_text(item.get("summary")) or maybe_text(item.get("statement")))
                ],
            }
        )
    return {
        "profile_id": maybe_text(plan_payload.get("profile_id")),
        "profile_summary": maybe_text(plan_payload.get("profile_summary")),
        "hypotheses": compact_hypotheses,
        "open_questions": [
            maybe_text(item) for item in plan_payload.get("open_questions", []) if maybe_text(item)
        ][:4],
    }


def _history_search_payload(
    *,
    db_path: Path,
    mission_payload: dict[str, Any],
    history_query: dict[str, Any],
    requested_top_k: int,
) -> dict[str, Any]:
    candidate_limit = normalize_history_candidate_limit(requested_top_k)
    return command_search_cases(
        argparse.Namespace(
            db=str(db_path),
            query=maybe_text(history_query.get("query")),
            region_label=maybe_text(history_query.get("region_label")),
            moderator_status="",
            evidence_sufficiency="",
            exclude_case_id=maybe_text(mission_payload.get("run_id")),
            profile_id=maybe_text(history_query.get("profile_id")),
            claim_types=list(history_query.get("claim_types", [])),
            metric_families=list(history_query.get("metric_families", [])),
            gap_types=list(history_query.get("gap_types", [])),
            source_skills=list(history_query.get("source_skills", [])),
            limit=candidate_limit,
            pretty=False,
        )
    )


def _overlap_values(left: list[Any], right: list[Any]) -> list[str]:
    return sorted(set(normalized_values(left)) & set(normalized_values(right)))


def _append_overlap_score(
    *,
    query_values: list[Any],
    candidate_values: list[Any],
    label: str,
    base_score: float,
    per_value_score: float,
    reasons: list[str],
) -> tuple[float, list[str]]:
    overlap = _overlap_values(query_values, candidate_values)
    if not overlap:
        return 0.0, []
    reasons.append(f"{label}:" + ",".join(overlap[:3]))
    return base_score + (per_value_score * len(overlap)), overlap


def _case_claim_lookup(bundle: dict[str, Any]) -> dict[str, dict[str, Any]]:
    claims = bundle.get("claims") if isinstance(bundle.get("claims"), list) else []
    lookup: dict[str, dict[str, Any]] = {}
    for claim in claims:
        if not isinstance(claim, dict):
            continue
        claim_id = maybe_text(claim.get("claim_id"))
        if not claim_id:
            continue
        lookup[claim_id] = {
            "claim_type": maybe_text(claim.get("claim_type")),
            "source_skills": parse_json_text(claim.get("public_source_skills_json"), default=[]),
        }
    return lookup


def _artifact_excerpt_candidates(
    *,
    retrieved_case: dict[str, Any],
    bundle: dict[str, Any],
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    claims_by_id = _case_claim_lookup(bundle)
    final_decision_summary = maybe_text(retrieved_case.get("final_decision_summary")) or maybe_text(retrieved_case.get("final_brief"))
    if final_decision_summary:
        candidates.append(
            {
                "artifact_kind": "decision-summary",
                "artifact_id": f"{maybe_text(retrieved_case.get('case_id'))}:final-decision",
                "round_id": maybe_text(retrieved_case.get("current_round_id")),
                "label": "final decision summary",
                "text": final_decision_summary,
                "claim_types": list(retrieved_case.get("matched_claim_types", [])),
                "metric_families": list(retrieved_case.get("matched_metric_families", [])),
                "gap_types": list(retrieved_case.get("final_missing_evidence_types", [])),
                "source_skills": list(retrieved_case.get("matched_source_skills", [])),
            }
        )

    rounds = bundle.get("rounds") if isinstance(bundle.get("rounds"), list) else []
    for round_item in rounds:
        if not isinstance(round_item, dict):
            continue
        decision_summary = maybe_text(round_item.get("decision_summary"))
        if not decision_summary:
            continue
        candidates.append(
            {
                "artifact_kind": "round-summary",
                "artifact_id": f"{maybe_text(retrieved_case.get('case_id'))}:{maybe_text(round_item.get('round_id'))}:round-summary",
                "round_id": maybe_text(round_item.get("round_id")),
                "label": "round decision summary",
                "text": decision_summary,
                "claim_types": list(retrieved_case.get("matched_claim_types", [])),
                "metric_families": list(retrieved_case.get("matched_metric_families", [])),
                "gap_types": parse_json_text(round_item.get("missing_evidence_types_json"), default=[]),
                "source_skills": list(retrieved_case.get("matched_source_skills", [])),
            }
        )

    reports = bundle.get("reports") if isinstance(bundle.get("reports"), list) else []
    for report in reports:
        if not isinstance(report, dict):
            continue
        summary = maybe_text(report.get("summary"))
        if not summary:
            continue
        role = maybe_text(report.get("role"))
        candidates.append(
            {
                "artifact_kind": "report-summary",
                "artifact_id": f"{maybe_text(retrieved_case.get('case_id'))}:{maybe_text(report.get('round_id'))}:{role or 'report'}",
                "round_id": maybe_text(report.get("round_id")),
                "role": role,
                "label": f"{role or 'report'} summary",
                "text": summary,
                "claim_types": list(retrieved_case.get("matched_claim_types", [])),
                "metric_families": list(retrieved_case.get("matched_metric_families", [])),
                "gap_types": list(retrieved_case.get("matched_gap_types", [])),
                "source_skills": list(retrieved_case.get("matched_source_skills", [])),
            }
        )

    evidence_items = bundle.get("evidence") if isinstance(bundle.get("evidence"), list) else []
    for evidence in evidence_items:
        if not isinstance(evidence, dict):
            continue
        summary = maybe_text(evidence.get("summary"))
        if not summary:
            continue
        claim_id = maybe_text(evidence.get("claim_id"))
        claim_details = claims_by_id.get(claim_id, {})
        candidates.append(
            {
                "artifact_kind": "evidence-card",
                "artifact_id": f"{maybe_text(retrieved_case.get('case_id'))}:{maybe_text(evidence.get('evidence_id'))}",
                "round_id": maybe_text(evidence.get("round_id")),
                "claim_id": claim_id,
                "label": "evidence card",
                "text": f"{maybe_text(evidence.get('verdict'))}/{maybe_text(evidence.get('confidence'))}: {summary}",
                "claim_types": [maybe_text(claim_details.get("claim_type"))] if maybe_text(claim_details.get("claim_type")) else [],
                "metric_families": list(retrieved_case.get("matched_metric_families", [])),
                "gap_types": parse_json_text(evidence.get("gaps_json"), default=[]),
                "source_skills": list(claim_details.get("source_skills", [])) if isinstance(claim_details.get("source_skills"), list) else [],
            }
        )

    claims = bundle.get("claims") if isinstance(bundle.get("claims"), list) else []
    for claim in claims:
        if not isinstance(claim, dict):
            continue
        summary = maybe_text(claim.get("summary")) or maybe_text(claim.get("statement"))
        if not summary:
            continue
        claim_type = maybe_text(claim.get("claim_type"))
        source_skills = parse_json_text(claim.get("public_source_skills_json"), default=[])
        candidates.append(
            {
                "artifact_kind": "curated-summary",
                "artifact_id": f"{maybe_text(retrieved_case.get('case_id'))}:{maybe_text(claim.get('claim_id'))}",
                "round_id": maybe_text(claim.get("round_id")),
                "claim_id": maybe_text(claim.get("claim_id")),
                "label": "curated claim summary",
                "text": summary,
                "claim_types": [claim_type] if claim_type else [],
                "metric_families": [],
                "gap_types": list(retrieved_case.get("matched_gap_types", [])),
                "source_skills": source_skills,
            }
        )
    return candidates


def _score_artifact_excerpt(
    *,
    retrieved_case: dict[str, Any],
    history_query: dict[str, Any],
    query_terms_list: list[str],
    candidate: dict[str, Any],
) -> dict[str, Any] | None:
    artifact_kind = maybe_text(candidate.get("artifact_kind"))
    snippet = truncate_text(candidate.get("text"), MAX_EXCERPT_CHARS)
    if not snippet:
        return None
    reasons: list[str] = []
    structured_score = 0.0
    lexical_score = 0.0

    if maybe_text(retrieved_case.get("case_profile_id")) and maybe_text(retrieved_case.get("case_profile_id")) == maybe_text(history_query.get("profile_id")):
        structured_score += 0.8
        reasons.append(f"case_profile:{maybe_text(retrieved_case.get('case_profile_id'))}")

    gap_score, gap_overlap = _append_overlap_score(
        query_values=list(history_query.get("gap_types", [])),
        candidate_values=list(candidate.get("gap_types", [])),
        label="artifact_gap_types",
        base_score=3.2 if artifact_kind == "evidence-card" else 2.1,
        per_value_score=1.2,
        reasons=reasons,
    )
    structured_score += gap_score

    claim_score, claim_overlap = _append_overlap_score(
        query_values=list(history_query.get("claim_types", [])),
        candidate_values=list(candidate.get("claim_types", [])),
        label="artifact_claim_types",
        base_score=3.0 if artifact_kind == "curated-summary" else 1.6,
        per_value_score=1.0,
        reasons=reasons,
    )
    structured_score += claim_score

    metric_score, metric_overlap = _append_overlap_score(
        query_values=list(history_query.get("metric_families", [])),
        candidate_values=list(candidate.get("metric_families", [])),
        label="artifact_metric_families",
        base_score=1.8 if artifact_kind in {"report-summary", "evidence-card"} else 1.0,
        per_value_score=0.8,
        reasons=reasons,
    )
    structured_score += metric_score

    source_score, source_overlap = _append_overlap_score(
        query_values=list(history_query.get("source_skills", [])),
        candidate_values=list(candidate.get("source_skills", [])),
        label="artifact_source_skills",
        base_score=1.2,
        per_value_score=0.5,
        reasons=reasons,
    )
    structured_score += source_score

    inherited_gap_overlap = list(retrieved_case.get("matched_gap_types", []))
    if artifact_kind in {"decision-summary", "round-summary", "evidence-card"} and inherited_gap_overlap:
        structured_score += 0.6 + (0.2 * len(inherited_gap_overlap))
        reasons.append("case_gap_context:" + ",".join(inherited_gap_overlap[:3]))
    inherited_claim_overlap = list(retrieved_case.get("matched_claim_types", []))
    if artifact_kind in {"curated-summary", "report-summary"} and inherited_claim_overlap:
        structured_score += 0.5 + (0.2 * len(inherited_claim_overlap))
        reasons.append("case_claim_context:" + ",".join(inherited_claim_overlap[:3]))
    inherited_metric_overlap = list(retrieved_case.get("matched_metric_families", []))
    if artifact_kind in {"report-summary", "evidence-card"} and inherited_metric_overlap:
        structured_score += 0.4 + (0.15 * len(inherited_metric_overlap))
        reasons.append("case_metric_context:" + ",".join(inherited_metric_overlap[:3]))

    snippet_terms = search_terms(snippet)
    lexical_hits = sorted(set(query_terms_list) & set(snippet_terms))
    if lexical_hits:
        lexical_score = 0.7 * float(len(lexical_hits))
        reasons.append("snippet:" + ",".join(lexical_hits[:3]))

    if structured_score <= 0.0 and lexical_score <= 0.0:
        return None

    total_score = round(structured_score + lexical_score + EXCERPT_KIND_BONUS.get(artifact_kind, 0.5), 3)
    return {
        "artifact_kind": artifact_kind,
        "artifact_id": maybe_text(candidate.get("artifact_id")),
        "round_id": maybe_text(candidate.get("round_id")),
        "role": maybe_text(candidate.get("role")),
        "claim_id": maybe_text(candidate.get("claim_id")),
        "label": maybe_text(candidate.get("label")),
        "snippet": snippet,
        "relevance_reasons": unique_strings(reasons)[:6],
        "score": {
            "match_tier": "artifact-structured" if structured_score > 0.0 else "artifact-lexical",
            "total": total_score,
            "components": {
                "structured_score": round(structured_score, 3),
                "lexical_score": round(lexical_score, 3),
                "kind_bonus": round(EXCERPT_KIND_BONUS.get(artifact_kind, 0.5), 3),
            },
        },
        "overlap": {
            "gap_types": gap_overlap,
            "claim_types": claim_overlap,
            "metric_families": metric_overlap,
            "source_skills": source_overlap,
        },
        "estimated_token_cost": _estimated_tokens(snippet, " ".join(reasons)),
    }


def _excerpt_sort_key(excerpt: dict[str, Any]) -> tuple[float, float, int, str]:
    score = excerpt.get("score") if isinstance(excerpt.get("score"), dict) else {}
    components = score.get("components") if isinstance(score.get("components"), dict) else {}
    return (
        -float(components.get("structured_score") or 0.0),
        -float(score.get("total") or 0.0),
        EXCERPT_KIND_PRIORITY.get(maybe_text(excerpt.get("artifact_kind")), 99),
        maybe_text(excerpt.get("artifact_id")),
    )


def _selected_case_payload(
    *,
    conn: Any,
    retrieved_case: dict[str, Any],
    history_query: dict[str, Any],
    query_terms_list: list[str],
    rank: int,
) -> dict[str, Any]:
    bundle = load_case_bundle(
        conn,
        maybe_text(retrieved_case.get("case_id")),
        include_reports=True,
        include_claims=True,
        include_evidence=True,
    )
    excerpt_candidates = _artifact_excerpt_candidates(retrieved_case=retrieved_case, bundle=bundle)
    scored_candidates = [
        scored
        for scored in (
            _score_artifact_excerpt(
                retrieved_case=retrieved_case,
                history_query=history_query,
                query_terms_list=query_terms_list,
                candidate=candidate,
            )
            for candidate in excerpt_candidates
        )
        if isinstance(scored, dict)
    ]
    selected_excerpts = sorted(scored_candidates, key=_excerpt_sort_key)[:MAX_EXCERPTS_PER_CASE]
    excerpts: list[dict[str, Any]] = []
    for excerpt_rank, excerpt in enumerate(selected_excerpts, start=1):
        item = dict(excerpt)
        item["excerpt_id"] = f"history-excerpt-{maybe_text(retrieved_case.get('case_id'))}-{excerpt_rank:02d}"
        item["rank"] = excerpt_rank
        excerpts.append(item)

    excerpt_budget = {
        "candidate_count": len(scored_candidates),
        "returned_count": len(excerpts),
        "truncated_by_cap": len(scored_candidates) > len(excerpts),
        "estimated_token_cost": sum(int(item.get("estimated_token_cost") or 0) for item in excerpts),
    }
    return {
        "rank": rank,
        "case_id": maybe_text(retrieved_case.get("case_id")),
        "score": float(retrieved_case.get("score") or 0.0),
        "score_components": retrieved_case.get("score_components", {}),
        "match_reasons": list(retrieved_case.get("match_reasons", [])),
        "topic": maybe_text(retrieved_case.get("topic")),
        "objective": maybe_text(retrieved_case.get("objective")),
        "region_label": maybe_text(retrieved_case.get("region_label")),
        "round_count": int(retrieved_case.get("round_count") or 0),
        "current_round_id": maybe_text(retrieved_case.get("current_round_id")),
        "final_moderator_status": maybe_text(retrieved_case.get("final_moderator_status")),
        "final_evidence_sufficiency": maybe_text(retrieved_case.get("final_evidence_sufficiency")),
        "final_missing_evidence_types": list(retrieved_case.get("final_missing_evidence_types", [])),
        "case_profile_id": maybe_text(retrieved_case.get("case_profile_id")),
        "matched_claim_types": list(retrieved_case.get("matched_claim_types", [])),
        "matched_metric_families": list(retrieved_case.get("matched_metric_families", [])),
        "matched_gap_types": list(retrieved_case.get("matched_gap_types", [])),
        "matched_source_skills": list(retrieved_case.get("matched_source_skills", [])),
        "excerpt_budget": excerpt_budget,
        "excerpts": excerpts,
    }


def build_history_retrieval_snapshot(
    *,
    run_dir: Path,
    round_id: str,
    db_path: Path,
    requested_top_k: int,
) -> dict[str, Any] | None:
    mission_payload = load_json_if_exists(mission_path(run_dir))
    if not isinstance(mission_payload, dict):
        return None
    plan_payload = _history_plan_payload(run_dir, round_id, mission_payload)
    history_query = _history_query_payload(mission_payload, plan_payload)
    plan_context = _compact_plan_context(plan_payload)
    search_payload = _history_search_payload(
        db_path=db_path,
        mission_payload=mission_payload,
        history_query=history_query,
        requested_top_k=requested_top_k,
    )
    candidate_cases = search_payload.get("cases") if isinstance(search_payload.get("cases"), list) else []
    selected_cases: list[dict[str, Any]] = []
    with connect_db(db_path) as conn:
        for rank, retrieved_case in enumerate(candidate_cases[:MAX_SURFACED_HISTORY_CASES], start=1):
            if not isinstance(retrieved_case, dict):
                continue
            selected_cases.append(
                _selected_case_payload(
                    conn=conn,
                    retrieved_case=retrieved_case,
                    history_query=history_query,
                    query_terms_list=list(search_payload.get("query_terms", [])),
                    rank=rank,
                )
            )
    selected_excerpt_count = sum(
        len(case.get("excerpts", []))
        for case in selected_cases
        if isinstance(case, dict)
    )
    estimated_token_cost = sum(
        int(case.get("excerpt_budget", {}).get("estimated_token_cost") or 0)
        for case in selected_cases
        if isinstance(case, dict)
    ) + _estimated_tokens(
        maybe_text(mission_payload.get("topic")),
        maybe_text(mission_payload.get("objective")),
        maybe_text(history_query.get("query")),
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "snapshot_id": f"history-retrieval-{round_id}",
        "run_id": maybe_text(mission_payload.get("run_id")),
        "round_id": round_id,
        "mission": {
            "topic": maybe_text(mission_payload.get("topic")),
            "objective": maybe_text(mission_payload.get("objective")),
            "region_label": maybe_text((mission_payload.get("region") or {}).get("label"))
            if isinstance(mission_payload.get("region"), dict)
            else "",
        },
        "history_query": history_query,
        "plan_context": plan_context,
        "budget": {
            "requested_top_k": normalize_history_candidate_limit(requested_top_k),
            "max_cases": MAX_SURFACED_HISTORY_CASES,
            "max_excerpts_per_case": MAX_EXCERPTS_PER_CASE,
            "candidate_case_count": len(candidate_cases),
            "selected_case_count": len(selected_cases),
            "selected_excerpt_count": selected_excerpt_count,
            "truncated_by_case_cap": len(candidate_cases) > len(selected_cases),
            "estimated_token_cost": estimated_token_cost,
        },
        "search": {
            "db": maybe_text(search_payload.get("db")),
            "query": maybe_text(search_payload.get("query")),
            "query_terms": list(search_payload.get("query_terms", [])),
            "region_label": maybe_text(search_payload.get("region_label")),
            "profile_id": maybe_text(search_payload.get("profile_id")),
            "claim_types": list(search_payload.get("claim_types", [])),
            "metric_families": list(search_payload.get("metric_families", [])),
            "gap_types": list(search_payload.get("gap_types", [])),
            "source_skills": list(search_payload.get("source_skills", [])),
        },
        "cases": selected_cases,
    }


def render_history_context_text_from_snapshot(snapshot: dict[str, Any]) -> str:
    if not isinstance(snapshot, dict):
        return ""
    cases = snapshot.get("cases") if isinstance(snapshot.get("cases"), list) else []
    if not cases:
        return ""
    mission = snapshot.get("mission") if isinstance(snapshot.get("mission"), dict) else {}
    history_query = snapshot.get("history_query") if isinstance(snapshot.get("history_query"), dict) else {}
    plan_context = snapshot.get("plan_context") if isinstance(snapshot.get("plan_context"), dict) else {}
    budget = snapshot.get("budget") if isinstance(snapshot.get("budget"), dict) else {}
    retrieval_focus_parts = []
    for field_name in ("claim_types", "metric_families", "gap_types", "source_skills"):
        values = history_query.get(field_name)
        if isinstance(values, list):
            text = ", ".join(maybe_text(item) for item in values if maybe_text(item))
            if text:
                retrieval_focus_parts.append(f"{field_name}={text}")
    lines = [
        "Compact historical-case context from the local eco-council case library.",
        "Use it only as planning guidance. Current-round evidence remains primary.",
        "Do not repeat exhausted fetch paths unless the region, time window, or claim mix is materially different.",
        "",
        f"Current topic: {maybe_text(mission.get('topic'))}",
        f"Current objective: {maybe_text(mission.get('objective'))}",
        f"Current region: {maybe_text(mission.get('region_label')) or 'n/a'}",
        f"Current investigation profile: {maybe_text(history_query.get('profile_id')) or maybe_text(plan_context.get('profile_id')) or 'n/a'}",
        "",
        (
            f"Retrieved similar cases: {int(budget.get('selected_case_count') or len(cases))} "
            f"(from {int(budget.get('candidate_case_count') or len(cases))} candidates; "
            f"max_cases={int(budget.get('max_cases') or MAX_SURFACED_HISTORY_CASES)}; "
            f"max_excerpts_per_case={int(budget.get('max_excerpts_per_case') or MAX_EXCERPTS_PER_CASE)}; "
            f"estimated_tokens={int(budget.get('estimated_token_cost') or 0)})"
        ),
    ]
    if retrieval_focus_parts:
        lines.extend(["Retrieval focus: " + "; ".join(retrieval_focus_parts)])
    priority_legs = history_query.get("priority_leg_ids") if isinstance(history_query.get("priority_leg_ids"), list) else []
    if priority_legs:
        lines.append("Priority legs: " + ", ".join(maybe_text(item) for item in priority_legs if maybe_text(item)))

    hypotheses = plan_context.get("hypotheses") if isinstance(plan_context.get("hypotheses"), list) else []
    if hypotheses:
        lines.extend(["", f"Primary hypotheses: {len(hypotheses)}"])
        for index, hypothesis in enumerate(hypotheses[:3], start=1):
            if not isinstance(hypothesis, dict):
                continue
            lines.append(f"{index}. {maybe_text(hypothesis.get('summary'))}")
            alternatives = hypothesis.get("alternative_hypotheses")
            if isinstance(alternatives, list) and alternatives:
                alt_text = "; ".join(maybe_text(item) for item in alternatives if maybe_text(item))
                if alt_text:
                    lines.append(f"   alternatives={alt_text}")

    open_questions = plan_context.get("open_questions") if isinstance(plan_context.get("open_questions"), list) else []
    if open_questions:
        lines.extend(["", "Open planning questions:"])
        for question in open_questions[:4]:
            if maybe_text(question):
                lines.append(f"- {maybe_text(question)}")

    for case in cases:
        if not isinstance(case, dict):
            continue
        missing_text = ", ".join(
            maybe_text(item) for item in case.get("final_missing_evidence_types", []) if maybe_text(item)
        )
        planning_overlap_parts = []
        if maybe_text(case.get("case_profile_id")):
            planning_overlap_parts.append(f"profile={maybe_text(case.get('case_profile_id'))}")
        for field_name in ("matched_claim_types", "matched_metric_families", "matched_gap_types", "matched_source_skills"):
            values = case.get(field_name)
            if not isinstance(values, list):
                continue
            text = ", ".join(maybe_text(item) for item in values if maybe_text(item))
            if text:
                planning_overlap_parts.append(f"{field_name}={text}")
        score_components = case.get("score_components") if isinstance(case.get("score_components"), dict) else {}
        lines.extend(
            [
                "",
                (
                    f"{int(case.get('rank') or 0)}. case_id={maybe_text(case.get('case_id'))}; "
                    f"score={case.get('score')}; "
                    f"match_tier={maybe_text(score_components.get('match_tier')) or 'n/a'}; "
                    f"region={maybe_text(case.get('region_label'))}; "
                    f"rounds={int(case.get('round_count') or 0)}; "
                    f"moderator_status={maybe_text(case.get('final_moderator_status')) or 'unknown'}; "
                    f"evidence={maybe_text(case.get('final_evidence_sufficiency')) or 'unknown'}"
                ),
                f"   topic={maybe_text(case.get('topic'))}",
            ]
        )
        if planning_overlap_parts:
            lines.append("   planning_overlap=" + "; ".join(planning_overlap_parts))
        if missing_text:
            lines.append(f"   missing_evidence_types={missing_text}")
        reasons = case.get("match_reasons")
        if isinstance(reasons, list) and reasons:
            lines.append("   match_reasons=" + ", ".join(maybe_text(item) for item in reasons if maybe_text(item)))
        for excerpt in case.get("excerpts", []):
            if not isinstance(excerpt, dict):
                continue
            role_suffix = f"; role={maybe_text(excerpt.get('role'))}" if maybe_text(excerpt.get("role")) else ""
            lines.append(
                f"   excerpt_{int(excerpt.get('rank') or 0)}="
                f"[{maybe_text(excerpt.get('artifact_kind'))}; round={maybe_text(excerpt.get('round_id'))}{role_suffix}] "
                f"{maybe_text(excerpt.get('snippet'))}"
            )
            relevance_reasons = excerpt.get("relevance_reasons")
            if isinstance(relevance_reasons, list) and relevance_reasons:
                lines.append(
                    "   excerpt_reasons=" + ", ".join(maybe_text(item) for item in relevance_reasons if maybe_text(item))
                )
    return "\n".join(lines)


def materialize_history_context_artifacts(
    run_dir: Path,
    state: dict[str, Any],
    round_id: str,
    *,
    pretty: bool = True,
) -> dict[str, Any] | None:
    history = state.get("history_context") if isinstance(state.get("history_context"), dict) else {}
    db_text = maybe_text(history.get("db"))
    history_path = history_context_path(run_dir, round_id)
    snapshot_path = history_retrieval_path(run_dir, round_id)
    if not db_text:
        if history_path.exists():
            history_path.unlink()
        if snapshot_path.exists():
            snapshot_path.unlink()
        return None

    db_path = Path(db_text).expanduser().resolve()
    if not db_path.exists():
        if history_path.exists():
            history_path.unlink()
        if snapshot_path.exists():
            snapshot_path.unlink()
        return None

    snapshot = build_history_retrieval_snapshot(
        run_dir=run_dir,
        round_id=round_id,
        db_path=db_path,
        requested_top_k=history.get("top_k", DEFAULT_HISTORY_TOP_K),
    )
    if not isinstance(snapshot, dict):
        if history_path.exists():
            history_path.unlink()
        if snapshot_path.exists():
            snapshot_path.unlink()
        return None

    write_json(snapshot_path, snapshot, pretty=pretty)
    history_text = render_history_context_text_from_snapshot(snapshot)
    if history_text:
        write_text(history_path, history_text)
    elif history_path.exists():
        history_path.unlink()

    return {
        "history_context_path": str(history_path) if history_text else "",
        "history_retrieval_path": str(snapshot_path),
        "selected_case_count": int(snapshot.get("budget", {}).get("selected_case_count") or 0),
        "selected_excerpt_count": int(snapshot.get("budget", {}).get("selected_excerpt_count") or 0),
        "estimated_token_cost": int(snapshot.get("budget", {}).get("estimated_token_cost") or 0),
    }


__all__ = [
    "MAX_EXCERPTS_PER_CASE",
    "MAX_SURFACED_HISTORY_CASES",
    "build_history_retrieval_snapshot",
    "materialize_history_context_artifacts",
    "normalize_history_candidate_limit",
    "render_history_context_text_from_snapshot",
]
