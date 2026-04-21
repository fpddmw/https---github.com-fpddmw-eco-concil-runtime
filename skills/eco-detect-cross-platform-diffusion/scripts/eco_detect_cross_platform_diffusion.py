#!/usr/bin/env python3
"""Detect issue-level cross-platform diffusion from formal/public linkage results."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
import re
import sys
from typing import Any

SKILL_NAME = "eco-detect-cross-platform-diffusion"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.analysis_plane import (  # noqa: E402
    load_formal_public_link_context,
    sync_diffusion_edge_result_set,
)
from eco_council_runtime.kernel.signal_plane_normalizer import (  # noqa: E402
    ensure_signal_plane_schema,
    resolved_canonical_object_kind,
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


def source_available(value: Any) -> bool:
    text = maybe_text(value)
    return bool(text) and not text.startswith("missing-")


def list_field(item: dict[str, Any], key: str) -> list[str]:
    values = item.get(key)
    if not isinstance(values, list):
        return []
    return [maybe_text(value) for value in values if maybe_text(value)]


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


def unique_artifact_refs(values: list[Any], limit: int = 24) -> list[dict[str, str]]:
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


def semantic_tokens(text: str) -> list[str]:
    tokens = re.findall(r"[a-z0-9]+", maybe_text(text).casefold())
    return [token for token in tokens if token not in STOPWORDS]


def parse_iso_utc(text: str) -> datetime | None:
    value = maybe_text(text)
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def platform_label(source_skill: str) -> str:
    value = maybe_text(source_skill)
    if value in FORMAL_SOURCE_SKILLS:
        return "regulationsgov"
    if value.startswith("youtube-"):
        return "youtube"
    if value.startswith("bluesky-"):
        return "bluesky"
    if value.startswith("gdelt-"):
        return "gdelt"
    return value or "unknown-platform"


def plane_label(
    *,
    plane: str,
    source_skill: str,
    signal_kind: str,
    canonical_object_kind: str,
) -> str:
    resolved_kind = resolved_canonical_object_kind(
        plane=plane,
        source_skill=source_skill,
        signal_kind=signal_kind,
        canonical_object_kind=canonical_object_kind,
    )
    if resolved_kind == "formal-comment-signal":
        return "formal"
    if resolved_kind == "environment-observation-signal":
        return "environment"
    return "public"


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


def signal_text(row: sqlite3.Row) -> str:
    parts = [
        maybe_text(row["title"]),
        maybe_text(row["body_text"]),
        maybe_text(row["author_name"]),
        maybe_text(row["channel_name"]),
    ]
    return " ".join(part for part in parts if part)


def load_normalized_signals(
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
        ensure_signal_plane_schema(connection)
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
            SELECT signal_id, plane, source_skill, signal_kind,
                   canonical_object_kind, title, body_text, url,
                   author_name, channel_name, published_at_utc, observed_at_utc,
                   captured_at_utc, artifact_path, record_locator
            FROM normalized_signals
            WHERE run_id = ?
              AND round_id = ?
              AND plane IN ('public', 'formal')
            ORDER BY plane, source_skill, signal_id
            """,
            (run_id, round_id),
        ).fetchall()
    finally:
        connection.close()

    signals: list[dict[str, Any]] = []
    for row in rows:
        first_seen_dt = (
            parse_iso_utc(maybe_text(row["published_at_utc"]))
            or parse_iso_utc(maybe_text(row["observed_at_utc"]))
            or parse_iso_utc(maybe_text(row["captured_at_utc"]))
            or datetime(1970, 1, 1, tzinfo=timezone.utc)
        )
        signals.append(
            {
                "signal_id": maybe_text(row["signal_id"]),
                "plane": maybe_text(row["plane"]),
                "source_skill": maybe_text(row["source_skill"]),
                "signal_kind": maybe_text(row["signal_kind"]),
                "platform": platform_label(maybe_text(row["source_skill"])),
                "plane_label": plane_label(
                    plane=maybe_text(row["plane"]),
                    source_skill=maybe_text(row["source_skill"]),
                    signal_kind=maybe_text(row["signal_kind"]),
                    canonical_object_kind=maybe_text(row["canonical_object_kind"]),
                ),
                "canonical_object_kind": resolved_canonical_object_kind(
                    plane=maybe_text(row["plane"]),
                    source_skill=maybe_text(row["source_skill"]),
                    signal_kind=maybe_text(row["signal_kind"]),
                    canonical_object_kind=maybe_text(row["canonical_object_kind"]),
                ),
                "title": maybe_text(row["title"]),
                "body_text": maybe_text(row["body_text"]),
                "url": maybe_text(row["url"]),
                "first_seen_utc": first_seen_dt.isoformat().replace("+00:00", "Z"),
                "first_seen_dt": first_seen_dt,
                "artifact_ref": signal_ref(row),
                "tokens": semantic_tokens(signal_text(row)),
            }
        )

    if not signals:
        warnings.append(
            {
                "code": "no-normalized-formal-public-signals",
                "message": "No normalized formal/public signal-plane rows were available for diffusion detection.",
            }
        )
    return signals, warnings


def issue_profiles_from_links(
    links: list[dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    profiles: dict[str, dict[str, Any]] = {}
    signal_issue_index: dict[str, str] = {}
    for link in links:
        if not isinstance(link, dict):
            continue
        issue_label = maybe_text(link.get("issue_label"))
        if not issue_label:
            continue
        profile = {
            "issue_label": issue_label,
            "issue_terms": unique_texts(
                list_field(link, "issue_terms")
                + semantic_tokens(issue_label.replace("-", " "))
            )[:16],
            "cluster_ids": unique_texts(list_field(link, "cluster_ids")),
            "claim_ids": unique_texts(list_field(link, "claim_ids")),
            "recommended_lane": maybe_text(link.get("recommended_lane"))
            or "mixed-review",
            "route_status": maybe_text(link.get("route_status"))
            or "mixed-routing-review",
            "evidence_refs": unique_artifact_refs(
                link.get("evidence_refs", [])
                if isinstance(link.get("evidence_refs"), list)
                else [],
                limit=16,
            ),
        }
        profiles[issue_label] = profile
        for signal_id in list_field(link, "formal_signal_ids") + list_field(
            link, "public_signal_ids"
        ):
            if signal_id and signal_id not in signal_issue_index:
                signal_issue_index[signal_id] = issue_label
    return profiles, signal_issue_index


def score_signal_for_issue(signal: dict[str, Any], profile: dict[str, Any]) -> float:
    token_set = {
        maybe_text(token)
        for token in signal.get("tokens", [])
        if maybe_text(token)
    }
    score = 0.0
    for term in profile.get("issue_terms", []):
        term_text = maybe_text(term)
        if term_text and term_text in token_set:
            score += 1.0
    return score


def assign_signal_issue(
    signal: dict[str, Any],
    *,
    signal_issue_index: dict[str, str],
    profiles: dict[str, dict[str, Any]],
) -> str:
    signal_id = maybe_text(signal.get("signal_id"))
    if signal_id and signal_id in signal_issue_index:
        return signal_issue_index[signal_id]
    best_label = ""
    best_score = 0.0
    for issue_label, profile in profiles.items():
        score = score_signal_for_issue(signal, profile)
        if score > best_score or (score == best_score and issue_label < best_label):
            best_label = issue_label
            best_score = score
    if best_score >= 2.0:
        return best_label
    return ""


def empty_platform_bucket(issue_label: str, platform: str, plane: str) -> dict[str, Any]:
    return {
        "issue_label": issue_label,
        "platform": platform,
        "plane_label": plane,
        "source_signal_ids": [],
        "source_skills": [],
        "examples": [],
        "artifact_refs": [],
        "first_seen_dt": None,
        "first_seen_utc": "",
    }


def update_platform_bucket(bucket: dict[str, Any], signal: dict[str, Any]) -> None:
    bucket["source_signal_ids"].append(maybe_text(signal.get("signal_id")))
    bucket["source_skills"].append(maybe_text(signal.get("source_skill")))
    title = maybe_text(signal.get("title"))
    if title:
        bucket["examples"].append(title)
    bucket["artifact_refs"].append(signal.get("artifact_ref", {}))
    signal_dt = signal.get("first_seen_dt")
    if isinstance(signal_dt, datetime):
        current_dt = bucket.get("first_seen_dt")
        if not isinstance(current_dt, datetime) or signal_dt < current_dt:
            bucket["first_seen_dt"] = signal_dt
            bucket["first_seen_utc"] = maybe_text(signal.get("first_seen_utc"))


def infer_edge_type(source_plane: str, target_plane: str) -> str:
    if source_plane == "public" and target_plane == "public":
        return "cross-public-diffusion"
    if source_plane == "public" and target_plane == "formal":
        return "public-to-formal-spillover"
    if source_plane == "formal" and target_plane == "public":
        return "formal-to-public-spillover"
    return "cross-formal-diffusion"


def temporal_relation(source_dt: datetime, target_dt: datetime) -> tuple[str, float]:
    delta_hours = abs((target_dt - source_dt).total_seconds()) / 3600.0
    if delta_hours < 1.0:
        return "same-window", round(delta_hours, 3)
    if target_dt >= source_dt:
        return "source-earlier", round(delta_hours, 3)
    return "target-earlier", round(delta_hours, 3)


def edge_confidence(
    *,
    source_count: int,
    target_count: int,
    delta_hours: float,
    claim_count: int,
    cluster_count: int,
) -> float:
    score = 0.52
    score += min(source_count, 3) * 0.05
    score += min(target_count, 3) * 0.05
    if delta_hours <= 48.0:
        score += 0.1
    if delta_hours <= 6.0:
        score += 0.05
    if claim_count > 0:
        score += 0.06
    if cluster_count > 0:
        score += 0.04
    return round(min(score, 0.96), 3)


def edge_summary(
    issue_label: str,
    edge_type: str,
    source_platform: str,
    target_platform: str,
    relation: str,
) -> str:
    return (
        f"Issue {issue_label} shows {edge_type} from {source_platform} to {target_platform} "
        f"with temporal relation {relation}."
    )


def detect_cross_platform_diffusion_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    formal_public_links_path: str,
    db_path: str,
    output_path: str,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_path(
        run_dir_path,
        output_path,
        f"analytics/diffusion_edges_{round_id}.json",
    )
    link_context = load_formal_public_link_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        formal_public_links_path=formal_public_links_path,
        db_path=db_path,
    )
    resolved_db_file = resolve_db_path(
        run_dir_path,
        maybe_text(link_context.get("db_path")) or db_path,
    )
    warnings = (
        link_context.get("warnings", [])
        if isinstance(link_context.get("warnings"), list)
        else []
    )
    links = (
        link_context.get("links", [])
        if isinstance(link_context.get("links"), list)
        else []
    )
    profiles, signal_issue_index = issue_profiles_from_links(links)
    signals, signal_warnings = load_normalized_signals(
        resolved_db_file,
        run_id=run_id,
        round_id=round_id,
    )
    warnings.extend(signal_warnings)

    grouped: dict[str, dict[str, dict[str, Any]]] = {}
    for signal in signals:
        issue_label = assign_signal_issue(
            signal,
            signal_issue_index=signal_issue_index,
            profiles=profiles,
        )
        if not issue_label:
            continue
        issue_group = grouped.setdefault(issue_label, {})
        platform = maybe_text(signal.get("platform")) or "unknown-platform"
        plane = maybe_text(signal.get("plane_label")) or "public"
        bucket = issue_group.get(platform)
        if bucket is None:
            bucket = empty_platform_bucket(issue_label, platform, plane)
            issue_group[platform] = bucket
        update_platform_bucket(bucket, signal)

    edges: list[dict[str, Any]] = []
    for issue_label, issue_group in grouped.items():
        profile = profiles.get(issue_label, {})
        platforms = list(issue_group.values())
        if len(platforms) < 2:
            continue
        platforms.sort(
            key=lambda item: (
                item.get("first_seen_dt") or datetime(1970, 1, 1, tzinfo=timezone.utc),
                maybe_text(item.get("platform")),
            )
        )
        for index, source_bucket in enumerate(platforms):
            for target_bucket in platforms[index + 1 :]:
                source_dt = source_bucket.get("first_seen_dt")
                target_dt = target_bucket.get("first_seen_dt")
                if not isinstance(source_dt, datetime) or not isinstance(
                    target_dt, datetime
                ):
                    continue
                ordered_source = source_bucket
                ordered_target = target_bucket
                if target_dt < source_dt:
                    ordered_source, ordered_target = target_bucket, source_bucket
                    source_dt, target_dt = target_dt, source_dt
                relation, delta_hours = temporal_relation(source_dt, target_dt)
                edge_type = infer_edge_type(
                    maybe_text(ordered_source.get("plane_label")),
                    maybe_text(ordered_target.get("plane_label")),
                )
                source_signal_ids = unique_texts(
                    ordered_source.get("source_signal_ids", [])
                )
                target_signal_ids = unique_texts(
                    ordered_target.get("source_signal_ids", [])
                )
                evidence_refs = unique_artifact_refs(
                    list(profile.get("evidence_refs", []))
                    + list(ordered_source.get("artifact_refs", []))
                    + list(ordered_target.get("artifact_refs", [])),
                    limit=20,
                )
                edge_id = "edge-" + stable_hash(
                    run_id,
                    round_id,
                    issue_label,
                    ordered_source.get("platform"),
                    ordered_target.get("platform"),
                    edge_type,
                )[:12]
                edges.append(
                    {
                        "schema_version": "n3.0",
                        "edge_id": edge_id,
                        "run_id": run_id,
                        "round_id": round_id,
                        "issue_label": issue_label,
                        "source_platform": maybe_text(ordered_source.get("platform")),
                        "target_platform": maybe_text(ordered_target.get("platform")),
                        "source_plane": maybe_text(ordered_source.get("plane_label")),
                        "target_plane": maybe_text(ordered_target.get("plane_label")),
                        "source_signal_ids": source_signal_ids,
                        "target_signal_ids": target_signal_ids,
                        "source_signal_count": len(source_signal_ids),
                        "target_signal_count": len(target_signal_ids),
                        "source_source_skills": unique_texts(
                            ordered_source.get("source_skills", [])
                        ),
                        "target_source_skills": unique_texts(
                            ordered_target.get("source_skills", [])
                        ),
                        "source_examples": unique_texts(
                            ordered_source.get("examples", [])
                        )[:3],
                        "target_examples": unique_texts(
                            ordered_target.get("examples", [])
                        )[:3],
                        "edge_type": edge_type,
                        "temporal_relation": relation,
                        "time_delta_hours": delta_hours,
                        "source_first_seen_utc": maybe_text(
                            ordered_source.get("first_seen_utc")
                        ),
                        "target_first_seen_utc": maybe_text(
                            ordered_target.get("first_seen_utc")
                        ),
                        "recommended_lane": maybe_text(profile.get("recommended_lane"))
                        or "mixed-review",
                        "route_status": maybe_text(profile.get("route_status"))
                        or "mixed-routing-review",
                        "cluster_ids": unique_texts(profile.get("cluster_ids", [])),
                        "claim_ids": unique_texts(profile.get("claim_ids", [])),
                        "confidence": edge_confidence(
                            source_count=len(source_signal_ids),
                            target_count=len(target_signal_ids),
                            delta_hours=delta_hours,
                            claim_count=len(profile.get("claim_ids", [])),
                            cluster_count=len(profile.get("cluster_ids", [])),
                        ),
                        "edge_summary": edge_summary(
                            issue_label,
                            edge_type,
                            maybe_text(ordered_source.get("platform")),
                            maybe_text(ordered_target.get("platform")),
                            relation,
                        ),
                        "evidence_refs": evidence_refs,
                    }
                )

    edge_type_counts: dict[str, int] = {}
    issue_edge_counts: dict[str, int] = {}
    for edge in edges:
        edge_type = maybe_text(edge.get("edge_type"))
        issue_label = maybe_text(edge.get("issue_label"))
        if edge_type:
            edge_type_counts[edge_type] = edge_type_counts.get(edge_type, 0) + 1
        if issue_label:
            issue_edge_counts[issue_label] = issue_edge_counts.get(issue_label, 0) + 1

    wrapper = {
        "schema_version": "n3.0",
        "skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "query_basis": {
            "formal_public_links_path": maybe_text(
                link_context.get("formal_public_links_file")
            ),
            "formal_public_links_source": maybe_text(
                link_context.get("formal_public_link_source")
            ),
            "selection_mode": "infer-cross-platform-issue-diffusion",
            "method": "cross-platform-diffusion-v1",
        },
        "formal_public_links_path": maybe_text(
            link_context.get("formal_public_links_file")
        ),
        "formal_public_links_source": maybe_text(
            link_context.get("formal_public_link_source")
        ),
        "observed_inputs": {
            "formal_public_links_present": bool(
                source_available(link_context.get("formal_public_link_source"))
            ),
            "formal_public_links_artifact_present": bool(
                link_context.get("formal_public_links_artifact_present")
            ),
            "signal_count": len(signals),
            "issue_count": len(profiles),
        },
        "input_analysis_sync": {
            "formal_public_link": link_context.get("analysis_sync", {}),
        },
        "edge_count": len(edges),
        "edge_type_counts": edge_type_counts,
        "issue_edge_counts": issue_edge_counts,
        "edges": edges,
    }
    write_json(output_file, wrapper)
    analysis_sync = sync_diffusion_edge_result_set(
        run_dir_path,
        expected_run_id=run_id,
        round_id=round_id,
        diffusion_edges_path=output_file,
        db_path=str(resolved_db_file),
    )
    wrapper["db_path"] = maybe_text(analysis_sync.get("db_path"))
    wrapper["analysis_sync"] = analysis_sync
    write_json(output_file, wrapper)

    artifact_refs = [
        {
            "signal_id": "",
            "artifact_path": str(output_file),
            "record_locator": "$.edges",
            "artifact_ref": f"{output_file}:$.edges",
        }
    ]
    for edge in edges[:8]:
        edge_id = maybe_text(edge.get("edge_id"))
        if not edge_id:
            continue
        artifact_refs.append(
            {
                "signal_id": "",
                "artifact_path": str(output_file),
                "record_locator": "$.edges[?(@.edge_id=='" + edge_id + "')]",
                "artifact_ref": f"{output_file}:edge:{edge_id}",
            }
        )

    if not edges:
        warnings.append(
            {
                "code": "no-diffusion-edges",
                "message": "No cross-platform diffusion edges were produced from the available linked issues and normalized signals.",
            }
        )

    board_gap_hints: list[str] = []
    if edge_type_counts.get("cross-public-diffusion", 0):
        board_gap_hints.append(
            "Some issues now clearly span multiple public platforms rather than a single isolated channel."
        )
    if edge_type_counts.get("public-to-formal-spillover", 0):
        board_gap_hints.append(
            "Some issues appear to move from open-platform discourse into formal participation channels."
        )
    if edge_type_counts.get("formal-to-public-spillover", 0):
        board_gap_hints.append(
            "Some issues appear to move from formal participation records into broader public discourse."
        )

    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "edge_count": len(edges),
            "issue_count": len(profiles),
            "db_path": maybe_text(analysis_sync.get("db_path")),
        },
        "receipt_id": "diffusion-receipt-"
        + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:20],
        "batch_id": "diffusionbatch-"
        + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [
            maybe_text(edge.get("edge_id"))
            for edge in edges
            if maybe_text(edge.get("edge_id"))
        ],
        "warnings": warnings,
        "analysis_sync": analysis_sync,
        "input_analysis_sync": wrapper.get("input_analysis_sync", {}),
        "board_handoff": {
            "candidate_ids": [
                maybe_text(edge.get("edge_id"))
                for edge in edges
                if maybe_text(edge.get("edge_id"))
            ],
            "evidence_refs": artifact_refs[:20],
            "gap_hints": board_gap_hints,
            "challenge_hints": (
                ["Review fast, low-latency diffusion edges before treating them as stable narrative spillover."]
                if any(float(edge.get("time_delta_hours") or 0.0) <= 6.0 for edge in edges)
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
        description="Detect issue-level cross-platform diffusion from formal/public linkage results."
    )
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--formal-public-links-path", default="")
    parser.add_argument("--db-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = detect_cross_platform_diffusion_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        formal_public_links_path=args.formal_public_links_path,
        db_path=args.db_path,
        output_path=args.output_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
