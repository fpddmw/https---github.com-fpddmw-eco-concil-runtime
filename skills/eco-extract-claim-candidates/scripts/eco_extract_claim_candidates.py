#!/usr/bin/env python3
"""Extract board-ready public claim candidates from local signal-plane storage."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "eco-extract-claim-candidates"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.analysis_plane import (  # noqa: E402
    sync_claim_candidate_result_set,
)
from eco_council_runtime.kernel.signal_plane_normalizer import (  # noqa: E402
    ensure_signal_plane_schema,
    resolved_canonical_object_kind,
)

STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "but",
    "by",
    "for",
    "from",
    "has",
    "have",
    "in",
    "into",
    "is",
    "it",
    "its",
    "of",
    "on",
    "or",
    "our",
    "that",
    "the",
    "their",
    "this",
    "to",
    "was",
    "were",
    "with",
    "today",
}

CLAIM_TYPE_ALIASES = {
    "hazard-impact": {"hazard-impact"},
    "verification": {"verification", "evidence-dispute"},
    "evidence-dispute": {"verification", "evidence-dispute"},
    "social-response": {"social-response", "trust-conflict", "representation-conflict"},
    "trust-conflict": {"social-response", "trust-conflict", "representation-conflict"},
    "procedure-legitimacy": {"procedure-legitimacy"},
    "cost-distribution": {"cost-distribution", "distributional-conflict"},
    "distributional-conflict": {"cost-distribution", "distributional-conflict"},
    "public-claim": {"public-claim"},
}

ISSUE_RULES: dict[str, tuple[str, ...]] = {
    "air-quality-smoke": (
        "smoke",
        "wildfire",
        "haze",
        "air quality",
        "pm2.5",
        "pollution",
    ),
    "heat-risk": ("heat", "temperature", "hot", "heatwave"),
    "flood-water": ("flood", "stormwater", "overflow", "rainfall", "precipitation"),
    "water-contamination": (
        "contamination",
        "toxic",
        "water quality",
        "drinking water",
        "chemical",
        "spill",
    ),
    "permit-process": (
        "permit",
        "hearing",
        "comment period",
        "rulemaking",
        "agency",
        "review process",
        "eis",
    ),
    "waste-facility": (
        "landfill",
        "incinerator",
        "waste facility",
        "dump",
        "plant",
    ),
    "energy-infrastructure": (
        "pipeline",
        "solar",
        "wind farm",
        "battery",
        "grid",
        "power plant",
    ),
    "representation-trust": (
        "community voice",
        "ignored",
        "misleading",
        "rumor",
        "trust",
        "representation",
    ),
}

CONCERN_RULES: dict[str, tuple[str, ...]] = {
    "health-safety": (
        "health",
        "asthma",
        "respiratory",
        "toxic",
        "unsafe",
        "dangerous",
    ),
    "ecology": (
        "ecosystem",
        "wildlife",
        "river",
        "forest",
        "water quality",
        "habitat",
    ),
    "cost-livelihood": (
        "cost",
        "expensive",
        "jobs",
        "business",
        "livelihood",
        "income",
    ),
    "procedure-governance": (
        "permit",
        "hearing",
        "rulemaking",
        "comment period",
        "transparency",
        "agency",
    ),
    "fairness-equity": (
        "justice",
        "equity",
        "unfair",
        "burden",
        "low-income",
        "community",
    ),
    "trust-credibility": (
        "official",
        "media",
        "misleading",
        "rumor",
        "false",
        "trust",
    ),
    "daily-life": (
        "school",
        "outdoor",
        "visibility",
        "commute",
        "home",
        "children",
    ),
}

ACTOR_RULES: dict[str, tuple[str, ...]] = {
    "agency": ("agency", "epa", "department", "regulator", "city hall"),
    "resident": ("resident", "neighbor", "family", "community", "people here"),
    "company": ("company", "developer", "operator", "industry", "utility"),
    "ngo": ("ngo", "activist", "environmental group", "coalition"),
    "expert": ("scientist", "researcher", "expert", "doctor", "professor"),
    "media": ("reporter", "news", "media", "channel", "press"),
}

CITATION_RULES: dict[str, tuple[str, ...]] = {
    "official-document": ("official", "agency", "permit", "rulemaking", "filing"),
    "scientific-study": ("study", "research", "scientist", "peer reviewed", "paper"),
    "news-report": ("report", "reported", "news", "article", "headline"),
    "firsthand-observation": ("i saw", "we saw", "look outside", "skyline", "smell", "visibility"),
    "personal-experience": ("my kids", "my family", "at my home", "my neighborhood"),
    "platform-amplification": ("viral", "repost", "shared", "trending"),
    "rumor-hearsay": ("heard that", "rumor", "someone said", "they say"),
}

STANCE_RULES: dict[str, tuple[str, ...]] = {
    "oppose": ("oppose", "against", "reject", "stop", "ban", "harmful", "unacceptable"),
    "support": ("support", "approve", "benefit", "needed", "necessary", "protect", "helps"),
    "verify": ("verify", "evidence", "data", "official", "confirmed", "check", "investigate", "whether", "unclear"),
    "report-impact": ("intense", "covered", "haze", "smoke over", "affected", "flooded", "hot", "today"),
}

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS normalized_signals (
    signal_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    round_id TEXT NOT NULL,
    plane TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    source_skill TEXT NOT NULL,
    signal_kind TEXT NOT NULL,
    external_id TEXT NOT NULL DEFAULT '',
    dedupe_key TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    body_text TEXT NOT NULL DEFAULT '',
    url TEXT NOT NULL DEFAULT '',
    author_name TEXT NOT NULL DEFAULT '',
    channel_name TEXT NOT NULL DEFAULT '',
    language TEXT NOT NULL DEFAULT '',
    query_text TEXT NOT NULL DEFAULT '',
    metric TEXT NOT NULL DEFAULT '',
    numeric_value REAL,
    unit TEXT NOT NULL DEFAULT '',
    published_at_utc TEXT NOT NULL DEFAULT '',
    observed_at_utc TEXT NOT NULL DEFAULT '',
    window_start_utc TEXT NOT NULL DEFAULT '',
    window_end_utc TEXT NOT NULL DEFAULT '',
    captured_at_utc TEXT NOT NULL DEFAULT '',
    latitude REAL,
    longitude REAL,
    bbox_json TEXT NOT NULL DEFAULT '{}',
    quality_flags_json TEXT NOT NULL DEFAULT '[]',
    engagement_json TEXT NOT NULL DEFAULT '{}',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    raw_json TEXT NOT NULL DEFAULT 'null',
    artifact_path TEXT NOT NULL,
    record_locator TEXT NOT NULL,
    artifact_sha256 TEXT NOT NULL DEFAULT ''
);
"""


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def truncate_text(value: Any, limit: int) -> str:
    text = maybe_text(value)
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    return text[: limit - 3].rstrip() + "..."


def pretty_json(data: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(data, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_run_dir(run_dir: str) -> Path:
    return Path(run_dir).expanduser().resolve()


def resolve_db_path(run_dir: Path, db_path: str) -> Path:
    text = maybe_text(db_path)
    if not text:
        return run_dir / "analytics" / "signal_plane.sqlite"
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def resolve_output_path(run_dir: Path, output_path: str, default_name: str) -> Path:
    text = maybe_text(output_path)
    if not text:
        return (run_dir / "analytics" / default_name).resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def connect_db(run_dir: Path, db_path: str) -> tuple[sqlite3.Connection, Path]:
    file_path = resolve_db_path(run_dir, db_path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(file_path)
    connection.row_factory = sqlite3.Row
    ensure_signal_plane_schema(connection)
    return connection, file_path


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def semantic_tokens(text: str) -> list[str]:
    tokens = re.findall(r"[a-z0-9]+", text.casefold())
    return [token for token in tokens if token not in STOPWORDS]


def semantic_fingerprint(text: str, *, limit: int = 8) -> str:
    tokens = semantic_tokens(text)
    if not tokens:
        return "empty"
    return "-".join(tokens[:limit])


def count_rule_hits(folded: str, terms: tuple[str, ...]) -> int:
    return sum(1 for term in terms if term in folded)


def top_rule_matches(
    folded: str,
    rules: dict[str, tuple[str, ...]],
    *,
    limit: int,
) -> list[str]:
    scored = [
        (count_rule_hits(folded, terms), label)
        for label, terms in rules.items()
        if count_rule_hits(folded, terms) > 0
    ]
    scored.sort(key=lambda item: (-item[0], item[1]))
    return [label for _, label in scored[:limit]]


def evidence_citation_types(text: str) -> list[str]:
    folded = maybe_text(text).casefold()
    values = top_rule_matches(folded, CITATION_RULES, limit=3)
    return values or ["uncited-platform-discourse"]


def concern_facets(text: str) -> list[str]:
    folded = maybe_text(text).casefold()
    values = top_rule_matches(folded, CONCERN_RULES, limit=4)
    if values:
        return values
    if "smoke" in folded or "wildfire" in folded:
        return ["health-safety", "daily-life"]
    return ["general-public-concern"]


def actor_hints(text: str) -> list[str]:
    folded = maybe_text(text).casefold()
    values = top_rule_matches(folded, ACTOR_RULES, limit=3)
    if values:
        return values
    return ["public-participants"]


def issue_hint(text: str) -> str:
    folded = maybe_text(text).casefold()
    matches = top_rule_matches(folded, ISSUE_RULES, limit=1)
    return matches[0] if matches else "general-public-controversy"


def issue_terms(text: str, primary_issue: str) -> list[str]:
    folded = maybe_text(text).casefold()
    matched_terms: list[str] = []
    for term in ISSUE_RULES.get(primary_issue, ()):
        if term in folded:
            matched_terms.extend(
                [part for part in re.findall(r"[a-z0-9]+", term.casefold()) if part not in STOPWORDS]
            )
    if matched_terms:
        deduped: list[str] = []
        seen: set[str] = set()
        for token in matched_terms:
            if token in seen:
                continue
            seen.add(token)
            deduped.append(token)
        return deduped[:4]
    return semantic_tokens(text)[:4]


def stance_hint(text: str) -> str:
    folded = maybe_text(text).casefold()
    scores = {
        label: count_rule_hits(folded, terms) for label, terms in STANCE_RULES.items()
    }
    ranked = sorted(
        [(score, label) for label, score in scores.items() if score > 0],
        key=lambda item: (-item[0], item[1]),
    )
    return ranked[0][1] if ranked else "unclear"


def verifiability_hint(text: str, *, primary_issue: str, concerns: list[str]) -> str:
    folded = maybe_text(text).casefold()
    if primary_issue in {
        "air-quality-smoke",
        "heat-risk",
        "flood-water",
        "water-contamination",
    }:
        return "empirical-observable"
    if primary_issue in {"permit-process"} or "procedure-governance" in concerns:
        return "procedural-record"
    if primary_issue in {"representation-trust"} or "trust-credibility" in concerns:
        return "discourse-representation"
    if "cost-livelihood" in concerns or "fairness-equity" in concerns:
        return "normative-distribution"
    if any(token in folded for token in ("forecast", "projection", "future", "model")):
        return "predictive-uncertainty"
    return "mixed-public-claim"


def dispute_type(
    *,
    primary_issue: str,
    concerns: list[str],
    citations: list[str],
    verification_hint: str,
) -> str:
    if verification_hint == "empirical-observable":
        return "impact-severity"
    if verification_hint == "procedural-record":
        return "governance-procedure"
    if verification_hint == "discourse-representation":
        return "representation-gap"
    if verification_hint == "normative-distribution":
        return "distributional-conflict"
    if "rumor-hearsay" in citations or "official-document" in citations:
        return "evidence-quality"
    if primary_issue == "representation-trust" or "trust-credibility" in concerns:
        return "trust-conflict"
    return "mixed-controversy"


def claim_type_from_profile(profile: dict[str, Any]) -> str:
    verification = maybe_text(profile.get("verifiability_hint"))
    concerns = profile.get("concern_facets", [])
    if verification == "empirical-observable":
        return "hazard-impact"
    if verification == "procedural-record":
        return "procedure-legitimacy"
    if verification == "discourse-representation":
        return "trust-conflict"
    if verification == "normative-distribution":
        return "cost-distribution"
    if maybe_text(profile.get("dispute_type")) == "evidence-quality":
        return "evidence-dispute"
    if isinstance(concerns, list) and "trust-credibility" in concerns:
        return "trust-conflict"
    return "public-claim"


def claim_type_matches_filter(requested: str, actual: str) -> bool:
    requested_value = maybe_text(requested)
    actual_value = maybe_text(actual)
    if not requested_value:
        return True
    allowed = CLAIM_TYPE_ALIASES.get(requested_value, {requested_value})
    return actual_value in allowed


def controversy_profile(text: str) -> dict[str, Any]:
    primary_issue = issue_hint(text)
    concerns = concern_facets(text)
    citations = evidence_citation_types(text)
    verification = verifiability_hint(
        text,
        primary_issue=primary_issue,
        concerns=concerns,
    )
    profile = {
        "issue_hint": primary_issue,
        "issue_terms": issue_terms(text, primary_issue),
        "stance_hint": stance_hint(text),
        "concern_facets": concerns,
        "actor_hints": actor_hints(text),
        "evidence_citation_types": citations,
        "verifiability_hint": verification,
    }
    profile["dispute_type"] = dispute_type(
        primary_issue=primary_issue,
        concerns=concerns,
        citations=citations,
        verification_hint=verification,
    )
    profile["claim_type"] = claim_type_from_profile(profile)
    profile["group_signature"] = "|".join(
        [
            maybe_text(profile["claim_type"]),
            maybe_text(profile["issue_hint"]),
            maybe_text(profile["stance_hint"]),
            ",".join(profile["concern_facets"][:2]),
        ]
    )
    return profile


def controversy_summary(profile: dict[str, Any], *, signal_count: int) -> str:
    issue_value = maybe_text(profile.get("issue_hint")).replace("-", " ")
    stance_value = maybe_text(profile.get("stance_hint")).replace("-", " ")
    concerns = profile.get("concern_facets", [])
    concern_value = ", ".join(
        maybe_text(item).replace("-", " ") for item in concerns[:2] if maybe_text(item)
    )
    concern_text = concern_value or "general public concern"
    return (
        f"Grouped {signal_count} public signals around {issue_value or 'a public issue'} "
        f"with a dominant {stance_value or 'unclear'} posture and concerns about {concern_text}."
    )


def signal_ref(row: sqlite3.Row) -> dict[str, str]:
    return {
        "signal_id": row["signal_id"],
        "artifact_path": maybe_text(row["artifact_path"]),
        "record_locator": maybe_text(row["record_locator"]),
        "artifact_ref": f"{row['artifact_path']}:{row['record_locator']}",
    }


def extract_claim_candidates_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    db_path: str,
    source_skill: str,
    claim_type: str,
    keyword_any: list[str],
    max_candidates: int,
    output_path: str,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    candidate_path = resolve_output_path(run_dir_path, output_path, f"claim_candidates_{round_id}.json")
    connection, db_file = connect_db(run_dir_path, db_path)
    try:
        rows = connection.execute(
            "SELECT * FROM normalized_signals WHERE plane = 'public' AND run_id = ? AND round_id = ? ORDER BY COALESCE(published_at_utc, captured_at_utc) DESC, signal_id",
            (run_id, round_id),
        ).fetchall()
    finally:
        connection.close()
    groups: dict[str, list[sqlite3.Row]] = {}
    wanted_claim_type = maybe_text(claim_type)
    keywords = [maybe_text(keyword).casefold() for keyword in keyword_any if maybe_text(keyword)]
    for row in rows:
        if (
            resolved_canonical_object_kind(
                plane="public",
                source_skill=maybe_text(row["source_skill"]),
                signal_kind=maybe_text(row["signal_kind"]),
                canonical_object_kind=maybe_text(row["canonical_object_kind"]),
            )
            != "public-discourse-signal"
        ):
            continue
        if maybe_text(source_skill) and maybe_text(row["source_skill"]) != maybe_text(source_skill):
            continue
        text = " ".join(part for part in (maybe_text(row["title"]), maybe_text(row["body_text"])) if part)
        if not text:
            continue
        if keywords and not any(keyword in text.casefold() for keyword in keywords):
            continue
        profile = controversy_profile(text)
        derived_claim_type = maybe_text(profile.get("claim_type"))
        if not claim_type_matches_filter(wanted_claim_type, derived_claim_type):
            continue
        group_key = maybe_text(profile.get("group_signature"))
        groups.setdefault(group_key, []).append(row)
    ordered_groups = sorted(groups.values(), key=lambda items: (-len(items), maybe_text(items[0]["signal_id"])))
    candidates: list[dict[str, Any]] = []
    for group in ordered_groups[: max(1, max_candidates)]:
        lead = group[0]
        source_text = " ".join(part for part in (maybe_text(lead["title"]), maybe_text(lead["body_text"])) if part)
        profile = controversy_profile(source_text)
        derived_claim_type = maybe_text(profile.get("claim_type"))
        candidate_id = "claimcand-" + stable_hash(
            run_id,
            round_id,
            derived_claim_type,
            maybe_text(profile.get("issue_hint")),
            maybe_text(profile.get("stance_hint")),
            maybe_text(profile.get("dispute_type")),
        )[:12]
        time_values = sorted(maybe_text(row["published_at_utc"]) for row in group if maybe_text(row["published_at_utc"]))
        concern_values = (
            profile.get("concern_facets", [])
            if isinstance(profile.get("concern_facets"), list)
            else []
        )
        actor_values = (
            profile.get("actor_hints", [])
            if isinstance(profile.get("actor_hints"), list)
            else []
        )
        citation_values = (
            profile.get("evidence_citation_types", [])
            if isinstance(profile.get("evidence_citation_types"), list)
            else []
        )
        issue_terms_value = (
            profile.get("issue_terms", [])
            if isinstance(profile.get("issue_terms"), list)
            else []
        )
        candidates.append(
            {
                "schema_version": "n2",
                "claim_id": candidate_id,
                "run_id": run_id,
                "round_id": round_id,
                "agent_role": "sociologist",
                "claim_type": derived_claim_type,
                "status": "candidate",
                "summary": truncate_text(lead["title"] or lead["body_text"], 180),
                "statement": truncate_text(source_text, 320),
                "issue_hint": maybe_text(profile.get("issue_hint")),
                "issue_terms": issue_terms_value,
                "stance_hint": maybe_text(profile.get("stance_hint")),
                "concern_facets": concern_values,
                "actor_hints": actor_values,
                "evidence_citation_types": citation_values,
                "verifiability_hint": maybe_text(profile.get("verifiability_hint")),
                "dispute_type": maybe_text(profile.get("dispute_type")),
                "controversy_seed": {
                    "issue_hint": maybe_text(profile.get("issue_hint")),
                    "stance_hint": maybe_text(profile.get("stance_hint")),
                    "concern_facets": concern_values,
                    "actor_hints": actor_values,
                    "evidence_citation_types": citation_values,
                    "verifiability_hint": maybe_text(profile.get("verifiability_hint")),
                    "dispute_type": maybe_text(profile.get("dispute_type")),
                },
                "time_window": {
                    "start_utc": time_values[0] if time_values else "",
                    "end_utc": time_values[-1] if time_values else "",
                },
                "place_scope": {"label": "Public evidence footprint", "geometry": {}},
                "claim_scope": {"label": "Public evidence footprint", "geometry": {}, "usable_for_matching": False},
                "source_signal_count": len(group),
                "source_signal_ids": [row["signal_id"] for row in group],
                "public_refs": [signal_ref(row) for row in group[:8]],
                "compact_audit": {
                    "representative": len(group) > 1,
                    "retained_count": min(len(group), 8),
                    "total_candidate_count": len(group),
                    "coverage_summary": controversy_summary(profile, signal_count=len(group)),
                    "concentration_flags": [],
                    "coverage_dimensions": ["issue-hint", "stance-hint", "concern-facets", "publication-time"],
                    "missing_dimensions": ["verification-route", "place-scope"],
                    "sampling_notes": [],
                },
            }
        )
    wrapper = {
        "schema_version": "n2",
        "skill": SKILL_NAME,
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "query_basis": {
            "source_plane": "public",
            "db_path": str(db_file),
            "source_skill_filter": maybe_text(source_skill),
            "claim_type_filter": wanted_claim_type,
            "keyword_any": [
                maybe_text(keyword) for keyword in keyword_any if maybe_text(keyword)
            ],
            "max_candidates": max(1, int(max_candidates)),
            "selection_mode": "group-by-issue-stance-concern-seed",
            "order_by": "COALESCE(published_at_utc, captured_at_utc) DESC, signal_id",
            "method": "controversy-seed-extraction-v1",
        },
        "candidate_count": len(candidates),
        "candidates": candidates,
    }
    write_json(candidate_path, wrapper)
    analysis_sync = sync_claim_candidate_result_set(
        run_dir_path,
        expected_run_id=run_id,
        round_id=round_id,
        claim_candidates_path=candidate_path,
        db_path=str(db_file),
    )
    wrapper["db_path"] = maybe_text(analysis_sync.get("db_path"))
    wrapper["analysis_sync"] = analysis_sync
    write_json(candidate_path, wrapper)
    artifact_refs = [{"signal_id": "", "artifact_path": str(candidate_path), "record_locator": "$.candidates", "artifact_ref": f"{candidate_path}:$.candidates"}]
    for candidate in candidates:
        artifact_refs.extend(candidate["public_refs"])
    batch_id = "candbatch-" + stable_hash(SKILL_NAME, run_id, round_id, str(candidate_path))[:16]
    warnings = [] if candidates else [{"code": "no-candidates", "message": "No claim candidates were extracted from the current public signal plane."}]
    return {
        "status": "completed",
        "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "candidate_count": len(candidates), "output_path": str(candidate_path), "db_path": maybe_text(analysis_sync.get("db_path"))},
        "receipt_id": "candidate-receipt-" + stable_hash(SKILL_NAME, batch_id)[:20],
        "batch_id": batch_id,
        "artifact_refs": artifact_refs[:40],
        "canonical_ids": [candidate["claim_id"] for candidate in candidates],
        "warnings": warnings,
        "analysis_sync": analysis_sync,
        "board_handoff": {
            "candidate_ids": [candidate["claim_id"] for candidate in candidates],
            "evidence_refs": artifact_refs[:20],
            "gap_hints": [
                "Issue and stance seeds still need clustering before they can support controversy mapping."
            ]
            if candidates
            else [],
            "challenge_hints": [
                "Review whether multiple public narratives with different stances were merged into one seed."
            ]
            if candidates
            else [],
            "suggested_next_skills": [
                "eco-cluster-claim-candidates",
                "eco-build-normalization-audit",
            ],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract board-ready public claim candidates from local signal-plane storage.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--db-path", default="")
    parser.add_argument("--source-skill", default="")
    parser.add_argument("--claim-type", default="")
    parser.add_argument("--keyword", action="append", default=[])
    parser.add_argument("--max-candidates", type=int, default=50)
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = extract_claim_candidates_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        db_path=args.db_path,
        source_skill=args.source_skill,
        claim_type=args.claim_type,
        keyword_any=args.keyword,
        max_candidates=args.max_candidates,
        output_path=args.output_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
