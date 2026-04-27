#!/usr/bin/env python3
"""Query compact historical cases from the cross-run case library."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
from pathlib import Path
from typing import Any

SKILL_NAME = "query-case-library"
SEARCH_TOKEN_RE = re.compile(r"[a-z0-9_]{2,}")
SEARCH_STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "into",
    "that",
    "this",
    "these",
    "those",
    "case",
    "archive",
    "historic",
    "historical",
    "investigation",
    "round",
    "evidence",
}

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS cases (
    case_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    run_dir TEXT NOT NULL,
    topic TEXT NOT NULL DEFAULT '',
    objective TEXT NOT NULL DEFAULT '',
    region_label TEXT NOT NULL DEFAULT '',
    profile_id TEXT NOT NULL DEFAULT '',
    publication_status TEXT NOT NULL DEFAULT '',
    promotion_status TEXT NOT NULL DEFAULT '',
    readiness_status TEXT NOT NULL DEFAULT '',
    last_round_id TEXT NOT NULL DEFAULT '',
    round_count INTEGER NOT NULL DEFAULT 0,
    final_decision_summary TEXT NOT NULL DEFAULT '',
    board_brief_excerpt TEXT NOT NULL DEFAULT '',
    history_summary_text TEXT NOT NULL DEFAULT '',
    claim_types_json TEXT NOT NULL DEFAULT '[]',
    metric_families_json TEXT NOT NULL DEFAULT '[]',
    gap_types_json TEXT NOT NULL DEFAULT '[]',
    source_skills_json TEXT NOT NULL DEFAULT '[]',
    alternative_hypotheses_json TEXT NOT NULL DEFAULT '[]',
    open_questions_json TEXT NOT NULL DEFAULT '[]',
    selected_evidence_refs_json TEXT NOT NULL DEFAULT '[]',
    mission_json TEXT NOT NULL DEFAULT '{}',
    archive_payload_json TEXT NOT NULL DEFAULT '{}',
    imported_at_utc TEXT NOT NULL
);
"""


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


def parse_json_text(raw: Any, default: Any) -> Any:
    if raw in (None, ""):
        return default
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return default
    return payload if isinstance(payload, type(default)) else default


def resolve_run_dir(run_dir: str) -> Path:
    return Path(run_dir).expanduser().resolve()


def default_archive_db_path(run_dir: Path) -> Path:
    return (run_dir / ".." / "archives" / "eco_case_library.sqlite").resolve()


def default_output_path(run_dir: Path, round_id: str) -> Path:
    return (run_dir / "archive" / f"case_library_query_{round_id}.json").resolve()


def resolve_path(run_dir: Path, override: str, default_path: Path) -> Path:
    text = maybe_text(override)
    if not text:
        return default_path
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def connect_archive_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.executescript(SCHEMA_SQL)
    return connection


def search_terms(*values: Any) -> list[str]:
    tokens: set[str] = set()
    for value in values:
        text = maybe_text(value).casefold()
        if not text:
            continue
        for token in SEARCH_TOKEN_RE.findall(text):
            if token not in SEARCH_STOPWORDS:
                tokens.add(token)
    return sorted(tokens)


def overlap(query_values: list[str], row_values: list[str]) -> list[str]:
    left = {maybe_text(value).casefold() for value in query_values if maybe_text(value)}
    right = {maybe_text(value).casefold() for value in row_values if maybe_text(value)}
    return sorted(left & right)


def region_score(query_region: str, row_region: str) -> tuple[float, list[str]]:
    query_text = maybe_text(query_region).casefold()
    row_text = maybe_text(row_region).casefold()
    if not query_text or not row_text:
        return 0.0, []
    if query_text == row_text:
        return 1.4, [f"region:{row_text}"]
    if query_text in row_text or row_text in query_text:
        return 0.8, [f"region-partial:{row_text}"]
    return 0.0, []


def lexical_score(query_terms: list[str], row: sqlite3.Row) -> tuple[float, list[str]]:
    haystack = " ".join([
        maybe_text(row["topic"]),
        maybe_text(row["objective"]),
        maybe_text(row["history_summary_text"]),
        maybe_text(row["board_brief_excerpt"]),
    ]).casefold()
    hits = [term for term in query_terms if term and term in haystack]
    if not hits:
        return 0.0, []
    return min(2.0, 0.35 * len(hits)), [f"lexical:{','.join(hits[:4])}"]


def match_tier(*, profile_match: bool, claim_overlap: list[str], metric_overlap: list[str], gap_overlap: list[str], source_overlap: list[str], region_weight: float, lexical_weight: float) -> str:
    structured_groups = int(profile_match) + int(bool(claim_overlap)) + int(bool(metric_overlap)) + int(bool(gap_overlap)) + int(bool(source_overlap))
    if structured_groups >= 2 or (profile_match and (claim_overlap or metric_overlap or gap_overlap)):
        return "structured-strong"
    if structured_groups >= 1:
        return "structured-weak"
    if region_weight > 0:
        return "region"
    if lexical_weight > 0:
        return "lexical"
    return "none"


def compact_result(
    row: sqlite3.Row,
    *,
    score: float,
    reasons: list[str],
    claim_overlap: list[str],
    metric_overlap: list[str],
    gap_overlap: list[str],
    source_overlap: list[str],
    tier: str,
) -> dict[str, Any]:
    return {
        "case_id": maybe_text(row["case_id"]),
        "run_id": maybe_text(row["run_id"]),
        "topic": maybe_text(row["topic"]),
        "objective": maybe_text(row["objective"]),
        "region_label": maybe_text(row["region_label"]),
        "profile_id": maybe_text(row["profile_id"]),
        "publication_status": maybe_text(row["publication_status"]),
        "promotion_status": maybe_text(row["promotion_status"]),
        "readiness_status": maybe_text(row["readiness_status"]),
        "history_summary_text": maybe_text(row["history_summary_text"]),
        "score": round(score, 3),
        "match_reasons": reasons,
        "matched_claim_types": claim_overlap,
        "matched_metric_families": metric_overlap,
        "matched_gap_types": gap_overlap,
        "matched_source_skills": source_overlap,
        "score_components": {
            "match_tier": tier,
            "profile_match": maybe_text(row["profile_id"]),
            "structured_overlap_count": len(claim_overlap) + len(metric_overlap) + len(gap_overlap) + len(source_overlap),
        },
        "preview_excerpt": maybe_text(row["history_summary_text"])[:240],
    }


def archive_ref_for_output(path: Path) -> dict[str, str]:
    return {
        "signal_id": "",
        "artifact_path": str(path),
        "record_locator": "$",
        "artifact_ref": f"{path}:$",
    }


def query_case_library_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    db_path: str,
    output_path: str,
    query_text: str,
    region_label: str,
    profile_id: str,
    claim_types: list[str],
    metric_families: list[str],
    gap_types: list[str],
    source_skills: list[str],
    exclude_case_id: str,
    limit: int,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    archive_db = resolve_path(run_dir_path, db_path, default_archive_db_path(run_dir_path))
    output_file = resolve_path(run_dir_path, output_path, default_output_path(run_dir_path, round_id))
    exclusion = maybe_text(exclude_case_id) or run_id
    query_terms = search_terms(query_text, region_label, profile_id, *claim_types, *metric_families, *gap_types, *source_skills)

    connection = connect_archive_db(archive_db)
    try:
        rows = connection.execute(
            "SELECT * FROM cases WHERE case_id != ? ORDER BY imported_at_utc DESC, case_id",
            (exclusion,),
        ).fetchall()
    finally:
        connection.close()

    scored_rows: list[tuple[float, dict[str, Any]]] = []
    for row in rows:
        row_claim_types = parse_json_text(row["claim_types_json"], [])
        row_metric_families = parse_json_text(row["metric_families_json"], [])
        row_gap_types = parse_json_text(row["gap_types_json"], [])
        row_source_skills = parse_json_text(row["source_skills_json"], [])
        claim_overlap = overlap(claim_types, row_claim_types)
        metric_overlap = overlap(metric_families, row_metric_families)
        gap_overlap = overlap(gap_types, row_gap_types)
        source_overlap = overlap(source_skills, row_source_skills)

        reasons: list[str] = []
        score = 0.0
        profile_match = bool(maybe_text(profile_id) and maybe_text(row["profile_id"]) == maybe_text(profile_id))
        if profile_match:
            score += 3.0
            reasons.append(f"profile:{maybe_text(row['profile_id'])}")
        if claim_overlap:
            score += 1.8 + (0.8 * len(claim_overlap))
            reasons.append(f"claim_types:{','.join(claim_overlap[:4])}")
        if metric_overlap:
            score += 1.5 + (0.6 * len(metric_overlap))
            reasons.append(f"metric_families:{','.join(metric_overlap[:4])}")
        if gap_overlap:
            score += 2.0 + (0.9 * len(gap_overlap))
            reasons.append(f"gap_types:{','.join(gap_overlap[:4])}")
        if source_overlap:
            score += 1.0 + (0.4 * len(source_overlap))
            reasons.append(f"source_skills:{','.join(source_overlap[:4])}")
        region_weight, region_reasons = region_score(region_label, row["region_label"])
        score += region_weight
        reasons.extend(region_reasons)
        lexical_weight, lexical_reasons = lexical_score(query_terms, row)
        score += lexical_weight
        reasons.extend(lexical_reasons)
        tier = match_tier(
            profile_match=profile_match,
            claim_overlap=claim_overlap,
            metric_overlap=metric_overlap,
            gap_overlap=gap_overlap,
            source_overlap=source_overlap,
            region_weight=region_weight,
            lexical_weight=lexical_weight,
        )
        if score <= 0 and query_terms:
            continue
        if tier == "none" and not query_terms and not any([profile_id, region_label, claim_types, metric_families, gap_types, source_skills]):
            tier = "lexical"
        scored_rows.append(
            (
                score,
                compact_result(
                    row,
                    score=score,
                    reasons=unique_texts(reasons),
                    claim_overlap=claim_overlap,
                    metric_overlap=metric_overlap,
                    gap_overlap=gap_overlap,
                    source_overlap=source_overlap,
                    tier=tier,
                ),
            )
        )

    scored_rows.sort(key=lambda item: (-item[0], item[1]["case_id"]))
    results = [item[1] for item in scored_rows[: max(1, limit)]]
    warnings = [] if results else [{"code": "no-results", "message": "No archived cases matched the supplied filters."}]
    payload = {
        "schema_version": "archive-case-query-v1",
        "skill": SKILL_NAME,
        "run_id": run_id,
        "round_id": round_id,
        "query": {
            "query_text": maybe_text(query_text),
            "region_label": maybe_text(region_label),
            "profile_id": maybe_text(profile_id),
            "claim_types": unique_texts(claim_types),
            "metric_families": unique_texts(metric_families),
            "gap_types": unique_texts(gap_types),
            "source_skills": unique_texts(source_skills),
            "exclude_case_id": exclusion,
        },
        "db_path": str(archive_db),
        "count": len(results),
        "cases": results,
    }
    write_json_file(output_file, payload)

    artifact_refs = [archive_ref_for_output(output_file)]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "db_path": str(archive_db),
            "output_path": str(output_file),
            "result_count": len(results),
        },
        "receipt_id": "archive-receipt-" + hashlib.sha256(f"{SKILL_NAME}|{run_id}|{round_id}|{output_file}".encode("utf-8")).hexdigest()[:20],
        "batch_id": "archivebatch-" + hashlib.sha256(f"{SKILL_NAME}|{run_id}|{round_id}|{len(results)}".encode("utf-8")).hexdigest()[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [maybe_text(result.get("case_id")) for result in results if maybe_text(result.get("case_id"))],
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": [maybe_text(result.get("case_id")) for result in results if maybe_text(result.get("case_id"))],
            "evidence_refs": artifact_refs,
            "gap_hints": [] if results else [warnings[0]["message"]],
            "challenge_hints": ["Archived cases provide analogy and precedent, not direct substitution for current evidence."] if results else [],
            "suggested_next_skills": ["materialize-history-context", "post-board-note", "propose-next-actions"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query compact historical cases from the cross-run case library.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--db-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--query-text", default="")
    parser.add_argument("--region-label", default="")
    parser.add_argument("--profile-id", default="")
    parser.add_argument("--claim-type", action="append", default=[])
    parser.add_argument("--metric-family", action="append", default=[])
    parser.add_argument("--gap-type", action="append", default=[])
    parser.add_argument("--source-skill", action="append", default=[])
    parser.add_argument("--exclude-case-id", default="")
    parser.add_argument("--limit", type=int, default=6)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = query_case_library_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        db_path=args.db_path,
        output_path=args.output_path,
        query_text=args.query_text,
        region_label=args.region_label,
        profile_id=args.profile_id,
        claim_types=args.claim_type,
        metric_families=args.metric_family,
        gap_types=args.gap_type,
        source_skills=args.source_skill,
        exclude_case_id=args.exclude_case_id,
        limit=args.limit,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())