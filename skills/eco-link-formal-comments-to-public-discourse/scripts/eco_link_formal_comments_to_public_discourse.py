#!/usr/bin/env python3
"""Link formal comment records and open-platform discourse around shared issues."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "eco-link-formal-comments-to-public-discourse"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.analysis_plane import (  # noqa: E402
    load_claim_candidate_context,
    load_claim_cluster_context,
    load_verification_route_context,
    sync_formal_public_link_result_set,
)

FORMAL_SOURCE_SKILLS = {
    "regulationsgov-comments-fetch",
    "regulationsgov-comment-detail-fetch",
}

STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "this",
    "to",
    "was",
    "with",
}

ISSUE_RULES: dict[str, tuple[str, ...]] = {
    "air-quality-smoke": (
        "smoke",
        "wildfire",
        "haze",
        "air quality",
        "pm25",
        "pm2.5",
        "pm 2.5",
        "respiratory",
    ),
    "heat-risk": ("heat", "temperature", "hot", "heatwave"),
    "flood-water": ("flood", "stormwater", "overflow", "rainfall", "precipitation"),
    "water-contamination": (
        "contamination",
        "water quality",
        "drinking water",
        "chemical",
        "spill",
        "toxic",
    ),
    "permit-process": (
        "permit",
        "hearing",
        "comment period",
        "rulemaking",
        "agency",
        "review process",
        "public comment",
        "docket",
    ),
    "representation-trust": (
        "community voice",
        "ignored",
        "trust",
        "representation",
        "not heard",
        "misleading",
        "voice",
        "voices",
        "residents say they were ignored",
    ),
}


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


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


def resolve_path(run_dir: Path, path_text: str, default_relative: str) -> Path:
    text = maybe_text(path_text)
    if not text:
        return (run_dir / default_relative).resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def resolve_db_path(run_dir: Path, db_path: str) -> Path:
    text = maybe_text(db_path)
    if not text:
        return (run_dir / "analytics" / "signal_plane.sqlite").resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def list_field(item: dict[str, Any], key: str) -> list[str]:
    values = item.get(key)
    if not isinstance(values, list):
        return []
    return [maybe_text(value) for value in values if maybe_text(value)]


def source_available(value: Any) -> bool:
    text = maybe_text(value)
    return bool(text) and not text.startswith("missing-")


def decode_json_text(text: str, default: Any) -> Any:
    try:
        return json.loads(text or json.dumps(default, ensure_ascii=True))
    except json.JSONDecodeError:
        return default


def semantic_tokens(text: str) -> list[str]:
    tokens = re.findall(r"[a-z0-9]+", maybe_text(text).casefold())
    return [token for token in tokens if token not in STOPWORDS]


def normalized_ref(value: Any) -> dict[str, str]:
    if isinstance(value, dict):
        artifact_path = maybe_text(value.get("artifact_path"))
        record_locator = maybe_text(value.get("record_locator"))
        artifact_ref = maybe_text(value.get("artifact_ref"))
        if not artifact_path and artifact_ref:
            marker = artifact_ref.find(":$")
            if marker >= 0:
                artifact_path = artifact_ref[:marker]
                if not record_locator:
                    record_locator = artifact_ref[marker + 1 :]
            else:
                artifact_path = artifact_ref
        if artifact_path and not artifact_ref:
            artifact_ref = (
                artifact_path
                if not record_locator
                else f"{artifact_path}:{record_locator}"
            )
        if not artifact_path:
            return {}
        return {
            "signal_id": maybe_text(value.get("signal_id")),
            "artifact_path": artifact_path,
            "record_locator": record_locator,
            "artifact_ref": artifact_ref or artifact_path,
        }
    text = maybe_text(value)
    if not text:
        return {}
    marker = text.find(":$")
    artifact_path = text[:marker] if marker >= 0 else text
    record_locator = text[marker + 1 :] if marker >= 0 else ""
    return {
        "signal_id": "",
        "artifact_path": artifact_path,
        "record_locator": record_locator,
        "artifact_ref": text,
    }


def unique_artifact_refs(values: list[Any], limit: int = 20) -> list[dict[str, str]]:
    seen: set[str] = set()
    results: list[dict[str, str]] = []
    for value in values:
        ref = normalized_ref(value)
        artifact_ref = maybe_text(ref.get("artifact_ref"))
        if not artifact_ref or artifact_ref in seen:
            continue
        seen.add(artifact_ref)
        results.append(ref)
        if len(results) >= limit:
            break
    return results


def issue_labels_from_text(text: str) -> list[str]:
    folded = maybe_text(text).casefold()
    ranked: list[tuple[int, str]] = []
    for label, terms in ISSUE_RULES.items():
        score = sum(1 for term in terms if term in folded)
        if score > 0:
            ranked.append((score, label))
    ranked.sort(key=lambda item: (-item[0], item[1]))
    return [label for _, label in ranked[:3]]


def issue_terms_for_label(issue_label: str) -> list[str]:
    values = [token for token in semantic_tokens(issue_label.replace("-", " "))]
    for term in ISSUE_RULES.get(maybe_text(issue_label), ()):
        values.extend(semantic_tokens(term))
    return unique_texts(values)


def default_lane_for_issue(issue_label: str) -> str:
    label = maybe_text(issue_label)
    if label in {
        "air-quality-smoke",
        "heat-risk",
        "flood-water",
        "water-contamination",
    }:
        return "environmental-observation"
    if label == "permit-process":
        return "formal-comment-and-policy-record"
    if label == "representation-trust":
        return "public-discourse-analysis"
    return "mixed-review"


def route_status_for_lane(lane: str) -> str:
    if lane == "environmental-observation":
        return "route-to-verification-lane"
    if lane == "formal-comment-and-policy-record":
        return "route-to-formal-record-review"
    if lane == "public-discourse-analysis":
        return "keep-in-public-discourse-analysis"
    if lane == "stakeholder-deliberation-analysis":
        return "keep-in-stakeholder-deliberation"
    return "mixed-routing-review"


def dominant_value(values: list[str], default: str) -> str:
    counts: dict[str, int] = {}
    for value in values:
        text = maybe_text(value)
        if not text:
            continue
        counts[text] = counts.get(text, 0) + 1
    if not counts:
        return default
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return ranked[0][0]


def signal_text(row: sqlite3.Row) -> str:
    metadata = decode_json_text(maybe_text(row["metadata_json"]), {})
    metadata_parts: list[str] = []
    if isinstance(metadata, dict):
        for key in ("docket_id", "agency_id", "query_text", "comment_on_id"):
            metadata_parts.append(maybe_text(metadata.get(key)))
    parts = [
        maybe_text(row["title"]),
        maybe_text(row["body_text"]),
        maybe_text(row["channel_name"]),
        maybe_text(row["author_name"]),
        " ".join(metadata_parts),
    ]
    return " ".join(part for part in parts if part)


def signal_ref(row: sqlite3.Row) -> dict[str, str]:
    artifact_path = maybe_text(row["artifact_path"])
    record_locator = maybe_text(row["record_locator"])
    return {
        "signal_id": maybe_text(row["signal_id"]),
        "artifact_path": artifact_path,
        "record_locator": record_locator,
        "artifact_ref": (
            artifact_path if not record_locator else f"{artifact_path}:{record_locator}"
        ),
    }


def load_normalized_public_signals(
    db_file: Path,
    *,
    run_id: str,
    round_id: str,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    warnings: list[dict[str, str]] = []
    if not db_file.exists():
        warnings.append(
            {
                "code": "missing-signal-plane-db",
                "message": f"Signal-plane database was not found at {db_file}.",
            }
        )
        return [], warnings

    connection = sqlite3.connect(db_file)
    connection.row_factory = sqlite3.Row
    try:
        table_exists = connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name = 'normalized_signals'
            """
        ).fetchone()
        if table_exists is None:
            warnings.append(
                {
                    "code": "missing-normalized-signals-table",
                    "message": "Signal-plane database exists but normalized_signals is unavailable.",
                }
            )
            return [], warnings

        rows = connection.execute(
            """
            SELECT signal_id, source_skill, signal_kind, title, body_text, url,
                   author_name, channel_name, metadata_json, artifact_path,
                   record_locator
            FROM normalized_signals
            WHERE run_id = ?
              AND round_id = ?
              AND plane = 'public'
            ORDER BY source_skill, signal_id
            """,
            (run_id, round_id),
        ).fetchall()
    finally:
        connection.close()

    results: list[dict[str, Any]] = []
    for row in rows:
        text = signal_text(row)
        results.append(
            {
                "signal_id": maybe_text(row["signal_id"]),
                "source_skill": maybe_text(row["source_skill"]),
                "signal_kind": maybe_text(row["signal_kind"]),
                "title": maybe_text(row["title"]),
                "body_text": maybe_text(row["body_text"]),
                "url": maybe_text(row["url"]),
                "author_name": maybe_text(row["author_name"]),
                "channel_name": maybe_text(row["channel_name"]),
                "artifact_ref": signal_ref(row),
                "is_formal": maybe_text(row["source_skill"]) in FORMAL_SOURCE_SKILLS,
                "text": text,
                "tokens": semantic_tokens(text),
                "issue_labels": issue_labels_from_text(text),
            }
        )

    if not results:
        warnings.append(
            {
                "code": "no-normalized-public-signals",
                "message": "No normalized public-plane signals were available for linkage.",
            }
        )
    return results, warnings


def ensure_issue_profile(
    profiles: dict[str, dict[str, Any]],
    issue_label: str,
) -> dict[str, Any]:
    label = maybe_text(issue_label) or "general-public-controversy"
    if label not in profiles:
        lane = default_lane_for_issue(label)
        profiles[label] = {
            "issue_label": label,
            "cluster_ids": [],
            "claim_ids": [],
            "terms": issue_terms_for_label(label),
            "concern_facets": [],
            "actor_hints": [],
            "recommended_lane_votes": [lane],
            "route_status_votes": [route_status_for_lane(lane)],
            "evidence_refs": [],
        }
    return profiles[label]


def build_issue_profiles(
    clusters: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
    routes: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    route_index: dict[str, list[dict[str, Any]]] = {}
    for route in routes:
        if not isinstance(route, dict):
            continue
        claim_id = maybe_text(route.get("claim_id"))
        if not claim_id:
            continue
        route_index.setdefault(claim_id, []).append(route)

    candidate_index: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        claim_id = maybe_text(candidate.get("claim_id"))
        if claim_id:
            candidate_index[claim_id] = candidate

    profiles: dict[str, dict[str, Any]] = {}
    attached_claim_ids: set[str] = set()
    for cluster in clusters:
        if not isinstance(cluster, dict):
            continue
        issue_label = (
            maybe_text(cluster.get("issue_label"))
            or maybe_text(cluster.get("cluster_label"))
            or maybe_text(cluster.get("claim_type"))
            or "general-public-controversy"
        )
        profile = ensure_issue_profile(profiles, issue_label)
        cluster_id = maybe_text(cluster.get("cluster_id"))
        if cluster_id:
            profile["cluster_ids"].append(cluster_id)
        profile["terms"].extend(
            semantic_tokens(maybe_text(cluster.get("cluster_label")))
        )
        profile["concern_facets"].extend(list_field(cluster, "concern_facets"))
        profile["actor_hints"].extend(list_field(cluster, "actor_hints"))
        profile["evidence_refs"].extend(
            cluster.get("public_refs", [])
            if isinstance(cluster.get("public_refs"), list)
            else []
        )
        for claim_id in list_field(cluster, "member_claim_ids"):
            attached_claim_ids.add(claim_id)
            profile["claim_ids"].append(claim_id)
            candidate = candidate_index.get(claim_id)
            if isinstance(candidate, dict):
                profile["terms"].extend(list_field(candidate, "issue_terms"))
                profile["terms"].extend(semantic_tokens(maybe_text(candidate.get("summary"))))
                profile["concern_facets"].extend(list_field(candidate, "concern_facets"))
                profile["actor_hints"].extend(list_field(candidate, "actor_hints"))
                profile["evidence_refs"].extend(
                    candidate.get("public_refs", [])
                    if isinstance(candidate.get("public_refs"), list)
                    else []
                )
            for route in route_index.get(claim_id, []):
                profile["recommended_lane_votes"].append(
                    maybe_text(route.get("recommended_lane"))
                )
                profile["route_status_votes"].append(
                    maybe_text(route.get("route_status"))
                )
                profile["evidence_refs"].extend(
                    route.get("evidence_refs", [])
                    if isinstance(route.get("evidence_refs"), list)
                    else []
                )

    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        claim_id = maybe_text(candidate.get("claim_id"))
        issue_label = (
            maybe_text(candidate.get("issue_hint"))
            or maybe_text(candidate.get("claim_type"))
            or "general-public-controversy"
        )
        profile = ensure_issue_profile(profiles, issue_label)
        if claim_id and claim_id not in attached_claim_ids:
            profile["claim_ids"].append(claim_id)
        profile["terms"].extend(list_field(candidate, "issue_terms"))
        profile["terms"].extend(semantic_tokens(maybe_text(candidate.get("summary"))))
        profile["concern_facets"].extend(list_field(candidate, "concern_facets"))
        profile["actor_hints"].extend(list_field(candidate, "actor_hints"))
        profile["evidence_refs"].extend(
            candidate.get("public_refs", [])
            if isinstance(candidate.get("public_refs"), list)
            else []
        )
        for route in route_index.get(claim_id, []):
            profile["recommended_lane_votes"].append(
                maybe_text(route.get("recommended_lane"))
            )
            profile["route_status_votes"].append(
                maybe_text(route.get("route_status"))
            )
            profile["evidence_refs"].extend(
                route.get("evidence_refs", [])
                if isinstance(route.get("evidence_refs"), list)
                else []
            )

    for issue_label, profile in profiles.items():
        lane = dominant_value(
            list_field(profile, "recommended_lane_votes"),
            default_lane_for_issue(issue_label),
        )
        profile["recommended_lane"] = lane
        profile["route_status"] = dominant_value(
            list_field(profile, "route_status_votes"),
            route_status_for_lane(lane),
        )
        profile["cluster_ids"] = unique_texts(profile.get("cluster_ids", []))
        profile["claim_ids"] = unique_texts(profile.get("claim_ids", []))
        profile["terms"] = unique_texts(profile.get("terms", []))[:12]
        profile["concern_facets"] = unique_texts(profile.get("concern_facets", []))[:6]
        profile["actor_hints"] = unique_texts(profile.get("actor_hints", []))[:6]
        profile["evidence_refs"] = unique_artifact_refs(
            profile.get("evidence_refs", []),
            limit=16,
        )
    return profiles


def score_signal_for_issue(signal: dict[str, Any], profile: dict[str, Any]) -> float:
    issue_label = maybe_text(profile.get("issue_label"))
    explicit_issue_labels = (
        signal.get("issue_labels") if isinstance(signal.get("issue_labels"), list) else []
    )
    score = 0.0
    if issue_label and issue_label in explicit_issue_labels:
        score += 5.0
    token_set = {
        maybe_text(token)
        for token in signal.get("tokens", [])
        if maybe_text(token)
    }
    for term in unique_texts(
        profile.get("terms", []) if isinstance(profile.get("terms"), list) else []
    ):
        term_text = maybe_text(term)
        if not term_text:
            continue
        if term_text in token_set:
            score += 1.0
    return score


def assign_signal_issue(
    signal: dict[str, Any],
    profiles: dict[str, dict[str, Any]],
) -> str:
    issue_labels = (
        signal.get("issue_labels") if isinstance(signal.get("issue_labels"), list) else []
    )
    explicit_label = maybe_text(issue_labels[0]) if issue_labels else ""
    if explicit_label and explicit_label not in profiles:
        return explicit_label

    best_label = ""
    best_score = 0.0
    for issue_label, profile in profiles.items():
        score = score_signal_for_issue(signal, profile)
        if score > best_score or (score == best_score and issue_label < best_label):
            best_label = issue_label
            best_score = score
    if explicit_label and explicit_label != best_label and best_score < 3.0:
        return explicit_label
    if best_score >= 2.0:
        return best_label
    return explicit_label


def link_status_for_issue(
    formal_count: int,
    public_count: int,
    claim_count: int,
) -> str:
    if formal_count > 0 and public_count > 0:
        return "aligned"
    if formal_count > 0:
        return "formal-only"
    if public_count > 0:
        return "public-only"
    if claim_count > 0:
        return "claim-side-only"
    return "unlinked"


def alignment_score(
    formal_count: int,
    public_count: int,
    *,
    has_claim_support: bool,
) -> float:
    if formal_count > 0 and public_count > 0:
        balance = min(formal_count, public_count) / max(formal_count, public_count)
        base = 0.55 + 0.35 * balance
        if has_claim_support:
            base += 0.1
        return round(min(base, 1.0), 3)
    if formal_count > 0 or public_count > 0:
        base = 0.25 + min(max(formal_count, public_count), 3) * 0.04
        if has_claim_support:
            base += 0.05
        return round(min(base, 0.45), 3)
    if has_claim_support:
        return 0.15
    return 0.0


def linkage_summary(
    issue_label: str,
    link_status: str,
    formal_count: int,
    public_count: int,
    lane: str,
) -> str:
    return (
        f"Issue {issue_label} shows {formal_count} formal signals and {public_count} public signals, "
        f"so it is marked as {link_status} and currently points to the "
        f"{lane.replace('-', ' ')} lane."
    )


def link_formal_comments_to_public_discourse_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    claim_cluster_path: str,
    claim_candidates_path: str,
    verification_route_path: str,
    output_path: str,
    db_path: str,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_path(
        run_dir_path,
        output_path,
        f"analytics/formal_public_links_{round_id}.json",
    )
    cluster_context = load_claim_cluster_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        claim_cluster_path=claim_cluster_path,
        db_path=db_path,
    )
    candidate_context = load_claim_candidate_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        claim_candidates_path=claim_candidates_path,
        db_path=maybe_text(cluster_context.get("db_path")) or db_path,
    )
    route_context = load_verification_route_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        verification_route_path=verification_route_path,
        db_path=maybe_text(candidate_context.get("db_path"))
        or maybe_text(cluster_context.get("db_path"))
        or db_path,
    )
    resolved_db_file = resolve_db_path(
        run_dir_path,
        maybe_text(route_context.get("db_path"))
        or maybe_text(candidate_context.get("db_path"))
        or maybe_text(cluster_context.get("db_path"))
        or db_path,
    )

    warnings = (
        cluster_context.get("warnings", [])
        if isinstance(cluster_context.get("warnings"), list)
        else []
    )
    warnings.extend(
        candidate_context.get("warnings", [])
        if isinstance(candidate_context.get("warnings"), list)
        else []
    )
    warnings.extend(
        route_context.get("warnings", [])
        if isinstance(route_context.get("warnings"), list)
        else []
    )

    clusters = (
        cluster_context.get("claim_clusters", [])
        if isinstance(cluster_context.get("claim_clusters"), list)
        else []
    )
    candidates = (
        candidate_context.get("claim_candidates", [])
        if isinstance(candidate_context.get("claim_candidates"), list)
        else []
    )
    routes = (
        route_context.get("routes", [])
        if isinstance(route_context.get("routes"), list)
        else []
    )

    issue_profiles = build_issue_profiles(clusters, candidates, routes)
    signal_rows, signal_warnings = load_normalized_public_signals(
        resolved_db_file,
        run_id=run_id,
        round_id=round_id,
    )
    warnings.extend(signal_warnings)

    grouped_signals: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for signal in signal_rows:
        issue_label = assign_signal_issue(signal, issue_profiles)
        if not issue_label:
            continue
        profile = ensure_issue_profile(issue_profiles, issue_label)
        issue_bucket = grouped_signals.setdefault(
            issue_label,
            {"formal": [], "public": []},
        )
        if bool(signal.get("is_formal")):
            issue_bucket["formal"].append(signal)
        else:
            issue_bucket["public"].append(signal)
        profile["terms"].extend(
            signal.get("tokens", [])
            if isinstance(signal.get("tokens"), list)
            else []
        )

    links: list[dict[str, Any]] = []
    for issue_label in sorted(issue_profiles.keys()):
        profile = issue_profiles[issue_label]
        issue_bucket = grouped_signals.get(issue_label, {"formal": [], "public": []})
        formal_signals = issue_bucket["formal"]
        public_signals = issue_bucket["public"]
        formal_ids = unique_texts([signal.get("signal_id") for signal in formal_signals])
        public_ids = unique_texts([signal.get("signal_id") for signal in public_signals])
        claim_ids = unique_texts(profile.get("claim_ids", []))
        cluster_ids = unique_texts(profile.get("cluster_ids", []))
        link_status = link_status_for_issue(
            len(formal_ids),
            len(public_ids),
            len(claim_ids),
        )
        if link_status == "unlinked":
            continue

        recommended_lane = (
            maybe_text(profile.get("recommended_lane"))
            or default_lane_for_issue(issue_label)
        )
        route_status = (
            maybe_text(profile.get("route_status"))
            or route_status_for_lane(recommended_lane)
        )
        signal_refs = [signal.get("artifact_ref", {}) for signal in formal_signals]
        signal_refs.extend(signal.get("artifact_ref", {}) for signal in public_signals)
        evidence_refs = unique_artifact_refs(
            list(profile.get("evidence_refs", [])) + signal_refs,
            limit=20,
        )
        linkage_id = "fplink-" + stable_hash(run_id, round_id, issue_label, link_status)[:12]
        links.append(
            {
                "schema_version": "n3.0",
                "linkage_id": linkage_id,
                "run_id": run_id,
                "round_id": round_id,
                "issue_label": issue_label,
                "issue_terms": unique_texts(profile.get("terms", []))[:12],
                "concern_facets": unique_texts(profile.get("concern_facets", []))[:6],
                "actor_hints": unique_texts(profile.get("actor_hints", []))[:6],
                "cluster_ids": cluster_ids,
                "claim_ids": claim_ids,
                "formal_signal_ids": formal_ids,
                "public_signal_ids": public_ids,
                "formal_signal_count": len(formal_ids),
                "public_signal_count": len(public_ids),
                "formal_source_skills": unique_texts(
                    [signal.get("source_skill") for signal in formal_signals]
                ),
                "public_source_skills": unique_texts(
                    [signal.get("source_skill") for signal in public_signals]
                ),
                "formal_examples": unique_texts(
                    [signal.get("title") for signal in formal_signals]
                )[:3],
                "public_examples": unique_texts(
                    [signal.get("title") for signal in public_signals]
                )[:3],
                "alignment_score": alignment_score(
                    len(formal_ids),
                    len(public_ids),
                    has_claim_support=bool(claim_ids),
                ),
                "link_status": link_status,
                "recommended_lane": recommended_lane,
                "route_status": route_status,
                "linkage_summary": linkage_summary(
                    issue_label,
                    link_status,
                    len(formal_ids),
                    len(public_ids),
                    recommended_lane,
                ),
                "evidence_refs": evidence_refs,
            }
        )

    link_status_counts: dict[str, int] = {}
    for link in links:
        link_status = maybe_text(link.get("link_status"))
        if not link_status:
            continue
        link_status_counts[link_status] = link_status_counts.get(link_status, 0) + 1

    formal_signal_total = sum(
        1 for signal in signal_rows if bool(signal.get("is_formal"))
    )
    public_signal_total = sum(
        1 for signal in signal_rows if not bool(signal.get("is_formal"))
    )

    wrapper = {
        "schema_version": "n3.0",
        "skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "query_basis": {
            "claim_cluster_path": maybe_text(cluster_context.get("claim_cluster_file")),
            "claim_candidates_path": maybe_text(
                candidate_context.get("claim_candidates_file")
            ),
            "verification_route_path": maybe_text(
                route_context.get("verification_route_file")
            ),
            "claim_cluster_source": maybe_text(
                cluster_context.get("claim_cluster_source")
            ),
            "claim_candidates_source": maybe_text(
                candidate_context.get("claim_candidate_source")
            ),
            "verification_route_source": maybe_text(
                route_context.get("verification_route_source")
            ),
            "selection_mode": "link-issue-level-formal-and-public-footprints",
            "method": "formal-public-issue-linkage-v1",
        },
        "claim_cluster_path": maybe_text(cluster_context.get("claim_cluster_file")),
        "claim_candidates_path": maybe_text(
            candidate_context.get("claim_candidates_file")
        ),
        "verification_route_path": maybe_text(
            route_context.get("verification_route_file")
        ),
        "claim_cluster_source": maybe_text(cluster_context.get("claim_cluster_source")),
        "claim_candidates_source": maybe_text(
            candidate_context.get("claim_candidate_source")
        ),
        "verification_route_source": maybe_text(
            route_context.get("verification_route_source")
        ),
        "observed_inputs": {
            "claim_cluster_present": bool(
                source_available(cluster_context.get("claim_cluster_source"))
            ),
            "claim_cluster_artifact_present": bool(
                cluster_context.get("claim_cluster_artifact_present")
            ),
            "claim_candidates_present": bool(
                source_available(candidate_context.get("claim_candidate_source"))
            ),
            "claim_candidates_artifact_present": bool(
                candidate_context.get("claim_candidates_artifact_present")
            ),
            "verification_route_present": bool(
                source_available(route_context.get("verification_route_source"))
            ),
            "verification_route_artifact_present": bool(
                route_context.get("verification_route_artifact_present")
            ),
            "formal_signal_count": formal_signal_total,
            "public_signal_count": public_signal_total,
        },
        "input_analysis_sync": {
            "claim_cluster": cluster_context.get("analysis_sync", {}),
            "claim_candidates": candidate_context.get("analysis_sync", {}),
            "verification_route": route_context.get("analysis_sync", {}),
        },
        "link_count": len(links),
        "link_status_counts": link_status_counts,
        "formal_signal_total": formal_signal_total,
        "public_signal_total": public_signal_total,
        "links": links,
    }
    write_json(output_file, wrapper)
    analysis_sync = sync_formal_public_link_result_set(
        run_dir_path,
        expected_run_id=run_id,
        round_id=round_id,
        formal_public_links_path=output_file,
        db_path=str(resolved_db_file),
    )
    wrapper["db_path"] = maybe_text(analysis_sync.get("db_path"))
    wrapper["analysis_sync"] = analysis_sync
    write_json(output_file, wrapper)

    artifact_refs = [
        {
            "signal_id": "",
            "artifact_path": str(output_file),
            "record_locator": "$.links",
            "artifact_ref": f"{output_file}:$.links",
        }
    ]
    for link in links[:8]:
        linkage_id = maybe_text(link.get("linkage_id"))
        if not linkage_id:
            continue
        artifact_refs.append(
            {
                "signal_id": "",
                "artifact_path": str(output_file),
                "record_locator": (
                    "$.links[?(@.linkage_id=='" + linkage_id + "')]"
                ),
                "artifact_ref": f"{output_file}:linkage:{linkage_id}",
            }
        )

    if not links:
        warnings.append(
            {
                "code": "no-formal-public-links",
                "message": "No issue-level formal/public linkages were produced from the available artifacts and signals.",
            }
        )

    board_gap_hints: list[str] = []
    if link_status_counts.get("formal-only", 0):
        board_gap_hints.append(
            "Some issues are visible in formal comments but have not surfaced in open-platform discourse."
        )
    if link_status_counts.get("public-only", 0):
        board_gap_hints.append(
            "Some issues are active in public discourse but have weak or missing formal participation traces."
        )
    if link_status_counts.get("claim-side-only", 0):
        board_gap_hints.append(
            "Some current claim-side issues still lack both formal-comment and open-platform linkage."
        )

    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "link_count": len(links),
            "formal_signal_total": formal_signal_total,
            "public_signal_total": public_signal_total,
            "db_path": maybe_text(analysis_sync.get("db_path")),
        },
        "receipt_id": "formalpublic-receipt-"
        + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:20],
        "batch_id": "formalpublicbatch-"
        + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [
            maybe_text(link.get("linkage_id"))
            for link in links
            if maybe_text(link.get("linkage_id"))
        ],
        "warnings": warnings,
        "analysis_sync": analysis_sync,
        "input_analysis_sync": wrapper.get("input_analysis_sync", {}),
        "board_handoff": {
            "candidate_ids": [
                maybe_text(link.get("linkage_id"))
                for link in links
                if maybe_text(link.get("linkage_id"))
            ],
            "evidence_refs": artifact_refs[:20],
            "gap_hints": board_gap_hints,
            "challenge_hints": (
                ["Review low-alignment issues before treating them as settled formal/public agreement."]
                if any(float(link.get("alignment_score") or 0.0) < 0.5 for link in links)
                else []
            ),
            "suggested_next_skills": [
                "eco-identify-representation-gaps",
                "eco-propose-next-actions",
                "eco-post-board-note",
            ],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Link formal comment records and open-platform discourse around shared issues."
    )
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--claim-cluster-path", default="")
    parser.add_argument("--claim-candidates-path", default="")
    parser.add_argument("--verification-route-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--db-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = link_formal_comments_to_public_discourse_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        claim_cluster_path=args.claim_cluster_path,
        claim_candidates_path=args.claim_candidates_path,
        verification_route_path=args.verification_route_path,
        output_path=args.output_path,
        db_path=args.db_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
