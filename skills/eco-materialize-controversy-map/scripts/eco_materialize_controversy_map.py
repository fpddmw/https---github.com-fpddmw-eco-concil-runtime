#!/usr/bin/env python3
"""Materialize a compact controversy map from the current claim-side chain."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "eco-materialize-controversy-map"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.analysis_plane import (  # noqa: E402
    load_claim_cluster_context,
    load_claim_scope_context,
    load_claim_verifiability_context,
    load_verification_route_context,
    sync_actor_profile_result_set,
    sync_concern_facet_result_set,
    sync_controversy_map_result_set,
    sync_evidence_citation_type_result_set,
    sync_issue_cluster_result_set,
    sync_stance_group_result_set,
)
from eco_council_runtime.analysis_objects import (  # noqa: E402
    normalize_actor_profile_payload,
    normalize_concern_facet_payload,
    normalize_controversy_map_payload,
    normalize_evidence_citation_type_payload,
    normalize_issue_cluster_payload,
    normalize_stance_group_payload,
)


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
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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
            artifact_ref = artifact_path if not record_locator else f"{artifact_path}:{record_locator}"
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


def unique_refs(values: list[Any]) -> list[dict[str, str]]:
    seen: set[str] = set()
    results: list[dict[str, str]] = []
    for value in values:
        ref = normalized_ref(value)
        artifact_ref = maybe_text(ref.get("artifact_ref"))
        if not artifact_ref or artifact_ref in seen:
            continue
        seen.add(artifact_ref)
        results.append(ref)
    return results


def count_labels(values: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        text = maybe_text(value)
        if not text:
            continue
        counts[text] = counts.get(text, 0) + 1
    return counts


def artifact_ref_for_path(path: Path, record_locator: str) -> dict[str, str]:
    locator = maybe_text(record_locator) or "$"
    return {
        "signal_id": "",
        "artifact_path": str(path),
        "record_locator": locator,
        "artifact_ref": f"{path}:{locator}",
    }


def sync_result_wrapper(
    run_dir_path: Path,
    *,
    run_id: str,
    round_id: str,
    output_file: Path,
    wrapper: dict[str, Any],
    db_path: str,
    sync_callable: Any,
    path_kwarg: str,
) -> dict[str, Any]:
    write_json(output_file, wrapper)
    analysis_sync = sync_callable(
        run_dir_path,
        expected_run_id=run_id,
        round_id=round_id,
        db_path=db_path,
        **{path_kwarg: output_file},
    )
    wrapper["db_path"] = maybe_text(analysis_sync.get("db_path"))
    wrapper["analysis_sync"] = analysis_sync
    write_json(output_file, wrapper)
    return analysis_sync


def build_typed_issue_surfaces(
    issue_clusters: list[dict[str, Any]],
    *,
    run_id: str,
    round_id: str,
    issue_clusters_file: Path,
    stance_groups_file: Path,
    concern_facets_file: Path,
    actor_profiles_file: Path,
    evidence_citation_types_file: Path,
) -> dict[str, list[dict[str, Any]]]:
    typed_issue_clusters: list[dict[str, Any]] = []
    stance_groups: list[dict[str, Any]] = []
    concern_facets: list[dict[str, Any]] = []
    actor_profiles: list[dict[str, Any]] = []
    citation_types: list[dict[str, Any]] = []
    for issue in issue_clusters:
        if not isinstance(issue, dict):
            continue
        issue_id = maybe_text(issue.get("map_issue_id")) or maybe_text(
            issue.get("cluster_id")
        )
        if not issue_id:
            continue
        claim_cluster_id = maybe_text(issue.get("cluster_id")) or issue_id
        claim_ids = unique_texts(list_field(issue, "claim_ids"))
        source_signal_ids = unique_texts(list_field(issue, "source_signal_ids"))
        concern_labels = unique_texts(list_field(issue, "concern_facets"))
        actor_labels = unique_texts(list_field(issue, "actor_hints"))
        citation_labels = unique_texts(list_field(issue, "evidence_citation_types"))
        evidence_refs = (
            issue.get("evidence_refs", [])
            if isinstance(issue.get("evidence_refs"), list)
            else []
        )
        lineage = issue.get("lineage", []) if isinstance(issue.get("lineage"), list) else []
        issue_label = maybe_text(issue.get("issue_label")) or "general-public-controversy"
        claim_type = maybe_text(issue.get("claim_type")) or "public-claim"
        dominant_stance = maybe_text(issue.get("dominant_stance")) or "unclear"
        recommended_lane = maybe_text(issue.get("recommended_lane")) or "mixed-review"
        route_status = maybe_text(issue.get("route_status")) or "mixed-routing-review"
        controversy_posture = maybe_text(issue.get("controversy_posture")) or "mixed-issue"
        member_count = int(issue.get("member_count") or len(claim_ids) or 1)
        aggregate_source_signal_count = int(
            issue.get("aggregate_source_signal_count") or len(source_signal_ids)
        )
        claim_scope_id = maybe_text(issue.get("claim_scope_id"))
        assessment_id = maybe_text(issue.get("assessment_id"))
        route_id = maybe_text(issue.get("route_id"))
        issue_provenance = (
            issue.get("provenance", {})
            if isinstance(issue.get("provenance"), dict)
            else {}
        )
        raw_stance_distribution = (
            issue.get("stance_distribution", [])
            if isinstance(issue.get("stance_distribution"), list)
            else []
        )
        normalized_stance_distribution: list[dict[str, Any]] = []
        for entry in raw_stance_distribution:
            if isinstance(entry, dict):
                stance_label = maybe_text(entry.get("stance")) or maybe_text(
                    entry.get("label")
                )
                stance_count = int(entry.get("count") or 0)
            else:
                stance_label = maybe_text(entry)
                stance_count = 0
            if not stance_label:
                continue
            if stance_count <= 0:
                stance_count = member_count if stance_label == dominant_stance else 1
            normalized_stance_distribution.append(
                {"stance": stance_label, "count": stance_count}
            )
        if not normalized_stance_distribution and dominant_stance:
            normalized_stance_distribution.append(
                {"stance": dominant_stance, "count": member_count}
            )

        stance_group_ids: list[str] = []
        for entry in normalized_stance_distribution:
            stance_label = maybe_text(entry.get("stance"))
            if not stance_label:
                continue
            stance_group_id = "stancegroup-" + stable_hash(
                run_id,
                round_id,
                issue_id,
                stance_label,
            )[:12]
            stance_group_ids.append(stance_group_id)
            stance_groups.append(
                normalize_stance_group_payload(
                    {
                        "stance_group_id": stance_group_id,
                        "run_id": run_id,
                        "round_id": round_id,
                        "cluster_id": issue_id,
                        "map_issue_id": issue_id,
                        "claim_cluster_id": claim_cluster_id,
                        "claim_scope_id": claim_scope_id,
                        "assessment_id": assessment_id,
                        "route_id": route_id,
                        "issue_label": issue_label,
                        "claim_type": claim_type,
                        "stance_label": stance_label,
                        "recommended_lane": recommended_lane,
                        "route_status": route_status,
                        "controversy_posture": controversy_posture,
                        "claim_ids": claim_ids,
                        "source_signal_ids": source_signal_ids,
                        "concern_facets": concern_labels,
                        "actor_hints": actor_labels,
                        "evidence_citation_types": citation_labels,
                        "member_count": int(entry.get("count") or 0),
                        "total_member_count": member_count,
                        "decision_source": maybe_text(issue.get("decision_source")),
                        "evidence_refs": evidence_refs,
                        "lineage": lineage,
                        "rationale": (
                            "Decomposed one controversy issue into a typed stance-group "
                            f"object for {issue_label}."
                        ),
                        "method": "controversy-typed-decomposition-v1",
                        "selection_mode": "derive-stance-groups-from-controversy-map",
                        "provenance": issue_provenance,
                    },
                    source_skill=SKILL_NAME,
                    artifact_path=str(stance_groups_file),
                )
            )

        concern_ids: list[str] = []
        for index, concern_label in enumerate(concern_labels):
            concern_id = "concern-" + stable_hash(
                run_id,
                round_id,
                issue_id,
                concern_label,
            )[:12]
            concern_ids.append(concern_id)
            concern_facets.append(
                normalize_concern_facet_payload(
                    {
                        "concern_id": concern_id,
                        "run_id": run_id,
                        "round_id": round_id,
                        "cluster_id": issue_id,
                        "map_issue_id": issue_id,
                        "claim_cluster_id": claim_cluster_id,
                        "claim_scope_id": claim_scope_id,
                        "assessment_id": assessment_id,
                        "route_id": route_id,
                        "issue_label": issue_label,
                        "claim_type": claim_type,
                        "concern_label": concern_label,
                        "priority": "primary" if index == 0 else "supporting",
                        "recommended_lane": recommended_lane,
                        "route_status": route_status,
                        "claim_ids": claim_ids,
                        "source_signal_ids": source_signal_ids,
                        "actor_hints": actor_labels,
                        "evidence_citation_types": citation_labels,
                        "affected_claim_count": member_count,
                        "source_signal_count": aggregate_source_signal_count,
                        "decision_source": maybe_text(issue.get("decision_source")),
                        "evidence_refs": evidence_refs,
                        "lineage": lineage,
                        "rationale": (
                            "Decomposed one controversy issue into a typed concern-facet "
                            f"object for {issue_label}."
                        ),
                        "method": "controversy-typed-decomposition-v1",
                        "selection_mode": "derive-concern-facets-from-controversy-map",
                        "provenance": issue_provenance,
                    },
                    source_skill=SKILL_NAME,
                    artifact_path=str(concern_facets_file),
                )
            )

        actor_ids: list[str] = []
        for actor_label in actor_labels:
            actor_id = "actor-" + stable_hash(
                run_id,
                round_id,
                issue_id,
                actor_label,
            )[:12]
            actor_ids.append(actor_id)
            actor_profiles.append(
                normalize_actor_profile_payload(
                    {
                        "actor_id": actor_id,
                        "run_id": run_id,
                        "round_id": round_id,
                        "cluster_id": issue_id,
                        "map_issue_id": issue_id,
                        "claim_cluster_id": claim_cluster_id,
                        "claim_scope_id": claim_scope_id,
                        "assessment_id": assessment_id,
                        "route_id": route_id,
                        "issue_label": issue_label,
                        "claim_type": claim_type,
                        "display_name": actor_label,
                        "actor_label": actor_label,
                        "dominant_stance": dominant_stance,
                        "recommended_lane": recommended_lane,
                        "route_status": route_status,
                        "claim_ids": claim_ids,
                        "source_signal_ids": source_signal_ids,
                        "concern_facets": concern_labels,
                        "evidence_citation_types": citation_labels,
                        "claim_count": member_count,
                        "source_signal_count": aggregate_source_signal_count,
                        "decision_source": maybe_text(issue.get("decision_source")),
                        "evidence_refs": evidence_refs,
                        "lineage": lineage,
                        "rationale": (
                            "Decomposed one controversy issue into a typed actor-profile "
                            f"object for {issue_label}."
                        ),
                        "method": "controversy-typed-decomposition-v1",
                        "selection_mode": "derive-actor-profiles-from-controversy-map",
                        "provenance": issue_provenance,
                    },
                    source_skill=SKILL_NAME,
                    artifact_path=str(actor_profiles_file),
                )
            )

        citation_type_ids: list[str] = []
        for citation_type in citation_labels:
            citation_type_id = "citationtype-" + stable_hash(
                run_id,
                round_id,
                issue_id,
                citation_type,
            )[:12]
            citation_type_ids.append(citation_type_id)
            citation_types.append(
                normalize_evidence_citation_type_payload(
                    {
                        "citation_type_id": citation_type_id,
                        "run_id": run_id,
                        "round_id": round_id,
                        "cluster_id": issue_id,
                        "map_issue_id": issue_id,
                        "claim_cluster_id": claim_cluster_id,
                        "claim_scope_id": claim_scope_id,
                        "assessment_id": assessment_id,
                        "route_id": route_id,
                        "issue_label": issue_label,
                        "claim_type": claim_type,
                        "citation_type": citation_type,
                        "dominant_stance": dominant_stance,
                        "recommended_lane": recommended_lane,
                        "route_status": route_status,
                        "claim_ids": claim_ids,
                        "source_signal_ids": source_signal_ids,
                        "concern_facets": concern_labels,
                        "actor_hints": actor_labels,
                        "claim_count": member_count,
                        "source_signal_count": aggregate_source_signal_count,
                        "decision_source": maybe_text(issue.get("decision_source")),
                        "evidence_refs": evidence_refs,
                        "lineage": lineage,
                        "rationale": (
                            "Decomposed one controversy issue into a typed evidence-citation-type "
                            f"object for {issue_label}."
                        ),
                        "method": "controversy-typed-decomposition-v1",
                        "selection_mode": "derive-citation-types-from-controversy-map",
                        "provenance": issue_provenance,
                    },
                    source_skill=SKILL_NAME,
                    artifact_path=str(evidence_citation_types_file),
                )
            )

        typed_issue_clusters.append(
            normalize_issue_cluster_payload(
                {
                    "cluster_id": issue_id,
                    "map_issue_id": issue_id,
                    "run_id": run_id,
                    "round_id": round_id,
                    "claim_cluster_id": claim_cluster_id,
                    "claim_scope_id": claim_scope_id,
                    "assessment_id": assessment_id,
                    "route_id": route_id,
                    "claim_ids": claim_ids,
                    "source_signal_ids": source_signal_ids,
                    "issue_label": issue_label,
                    "claim_type": claim_type,
                    "dominant_stance": dominant_stance,
                    "stance_distribution": normalized_stance_distribution,
                    "stance_group_ids": stance_group_ids,
                    "concern_ids": concern_ids,
                    "actor_ids": actor_ids,
                    "citation_type_ids": citation_type_ids,
                    "concern_facets": concern_labels,
                    "actor_hints": actor_labels,
                    "evidence_citation_types": citation_labels,
                    "verifiability_kind": maybe_text(issue.get("verifiability_kind")),
                    "dispute_type": maybe_text(issue.get("dispute_type")),
                    "recommended_lane": recommended_lane,
                    "route_status": route_status,
                    "controversy_posture": controversy_posture,
                    "member_count": member_count,
                    "aggregate_source_signal_count": aggregate_source_signal_count,
                    "confidence": issue.get("confidence"),
                    "issue_summary": maybe_text(issue.get("controversy_summary")),
                    "controversy_summary": maybe_text(issue.get("controversy_summary")),
                    "should_query_environment": bool(
                        issue.get("should_query_environment")
                    ),
                    "decision_source": maybe_text(issue.get("decision_source")),
                    "evidence_refs": evidence_refs,
                    "lineage": lineage,
                    "rationale": (
                        "Projected one controversy-map issue into a canonical issue-cluster "
                        f"object for {issue_label}."
                    ),
                    "method": "controversy-typed-decomposition-v1",
                    "selection_mode": "derive-issue-cluster-from-controversy-map",
                    "provenance": issue_provenance,
                },
                source_skill=SKILL_NAME,
                artifact_path=str(issue_clusters_file),
            )
        )
    return {
        "issue_clusters": typed_issue_clusters,
        "stance_groups": stance_groups,
        "concern_facets": concern_facets,
        "actor_profiles": actor_profiles,
        "citation_types": citation_types,
    }


def source_available(value: Any) -> bool:
    text = maybe_text(value)
    return bool(text) and not text.startswith("missing-")


def fallback_clusters_from_scopes(scopes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    clusters: list[dict[str, Any]] = []
    for scope in scopes:
        if not isinstance(scope, dict):
            continue
        claim_id = maybe_text(scope.get("claim_id"))
        issue_hint = maybe_text(scope.get("issue_hint")) or maybe_text(scope.get("claim_type")) or "general-public-controversy"
        clusters.append(
            {
                "cluster_id": claim_id,
                "claim_type": maybe_text(scope.get("claim_type")),
                "cluster_label": issue_hint,
                "issue_label": issue_hint,
                "dominant_stance": "unclear",
                "stance_distribution": [],
                "concern_facets": list_field(scope, "concern_facets"),
                "actor_hints": list_field(scope, "actor_hints"),
                "evidence_citation_types": list_field(scope, "evidence_citation_types"),
                "verifiability_posture": maybe_text(scope.get("verifiability_kind")),
                "dispute_type": maybe_text(scope.get("dispute_type")),
                "member_claim_ids": [claim_id] if claim_id else [],
                "member_count": 1,
                "aggregate_source_signal_count": 0,
                "source_signal_ids": list_field(scope, "source_signal_ids"),
                "evidence_refs": scope.get("evidence_refs", [])
                if isinstance(scope.get("evidence_refs"), list)
                else [],
                "public_refs": scope.get("evidence_refs", [])
                if isinstance(scope.get("evidence_refs"), list)
                else [],
                "controversy_summary": maybe_text(scope.get("matching_eligibility_reason")),
                "lineage": scope.get("lineage", [])
                if isinstance(scope.get("lineage"), list)
                else [],
            }
        )
    return clusters


def pick_claim_key(cluster: dict[str, Any]) -> str:
    cluster_id = maybe_text(cluster.get("cluster_id"))
    if cluster_id:
        return cluster_id
    member_claim_ids = list_field(cluster, "member_claim_ids")
    return member_claim_ids[0] if member_claim_ids else ""


def route_from_scope(scope: dict[str, Any]) -> tuple[str, str, bool]:
    lane = maybe_text(scope.get("required_evidence_lane")) or "route-before-matching"
    if lane == "environmental-observation":
        return "route-to-verification-lane", lane, bool(
            (scope.get("claim_scope") or {}).get("usable_for_matching")
        )
    if lane == "formal-comment-and-policy-record":
        return "route-to-formal-record-review", lane, False
    if lane == "public-discourse-analysis":
        return "keep-in-public-discourse-analysis", lane, False
    if lane == "stakeholder-deliberation-analysis":
        return "keep-in-stakeholder-deliberation", lane, False
    return "mixed-routing-review", lane, False


def controversy_posture(route_status: str, lane: str) -> str:
    if lane == "environmental-observation":
        return "empirical-issue"
    if route_status in {
        "route-to-formal-record-review",
        "keep-in-public-discourse-analysis",
        "keep-in-stakeholder-deliberation",
    }:
        return "non-empirical-issue"
    return "mixed-issue"


def issue_summary(cluster: dict[str, Any], lane: str, route_status: str) -> str:
    issue_label = maybe_text(cluster.get("issue_label")) or maybe_text(cluster.get("cluster_label")) or "public controversy"
    stance = maybe_text(cluster.get("dominant_stance")) or "unclear"
    return (
        f"Issue {issue_label} currently carries a dominant {stance} posture and is routed as "
        f"{route_status.replace('-', ' ')} via {lane.replace('-', ' ')}."
    )


def materialize_controversy_map_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    claim_cluster_path: str,
    claim_scope_path: str,
    claim_verifiability_path: str,
    verification_route_path: str,
    output_path: str,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_path(
        run_dir_path,
        output_path,
        f"analytics/controversy_map_{round_id}.json",
    )
    cluster_context = load_claim_cluster_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        claim_cluster_path=claim_cluster_path,
    )
    scope_context = load_claim_scope_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        claim_scope_path=claim_scope_path,
        db_path=maybe_text(cluster_context.get("db_path")),
    )
    verifiability_context = load_claim_verifiability_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        claim_verifiability_path=claim_verifiability_path,
        db_path=maybe_text(scope_context.get("db_path"))
        or maybe_text(cluster_context.get("db_path")),
    )
    route_context = load_verification_route_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        verification_route_path=verification_route_path,
        db_path=maybe_text(verifiability_context.get("db_path"))
        or maybe_text(scope_context.get("db_path"))
        or maybe_text(cluster_context.get("db_path")),
    )

    warnings: list[dict[str, Any]] = []
    for context in (
        cluster_context,
        scope_context,
        verifiability_context,
        route_context,
    ):
        warnings.extend(
            context.get("warnings", [])
            if isinstance(context.get("warnings"), list)
            else []
        )

    claim_scopes = (
        scope_context.get("claim_scopes", [])
        if isinstance(scope_context.get("claim_scopes"), list)
        else []
    )
    claim_clusters = (
        cluster_context.get("claim_clusters", [])
        if isinstance(cluster_context.get("claim_clusters"), list)
        else []
    )
    if not claim_clusters:
        claim_clusters = fallback_clusters_from_scopes(claim_scopes)
    assessments = (
        verifiability_context.get("assessments", [])
        if isinstance(verifiability_context.get("assessments"), list)
        else []
    )
    routes = (
        route_context.get("routes", [])
        if isinstance(route_context.get("routes"), list)
        else []
    )

    scope_by_claim_id = {
        maybe_text(scope.get("claim_id")): scope
        for scope in claim_scopes
        if isinstance(scope, dict) and maybe_text(scope.get("claim_id"))
    }
    assessment_by_claim_id = {
        maybe_text(item.get("claim_id")): item
        for item in assessments
        if isinstance(item, dict) and maybe_text(item.get("claim_id"))
    }
    route_by_claim_id = {
        maybe_text(item.get("claim_id")): item
        for item in routes
        if isinstance(item, dict) and maybe_text(item.get("claim_id"))
    }

    issue_clusters: list[dict[str, Any]] = []
    for cluster in claim_clusters:
        if not isinstance(cluster, dict):
            continue
        claim_key = pick_claim_key(cluster)
        scope = scope_by_claim_id.get(claim_key, {})
        assessment = assessment_by_claim_id.get(claim_key, {})
        route = route_by_claim_id.get(claim_key, {})
        fallback_status, fallback_lane, fallback_env = route_from_scope(scope)
        lane = (
            maybe_text(route.get("recommended_lane"))
            or maybe_text(assessment.get("recommended_lane"))
            or fallback_lane
        )
        route_status = maybe_text(route.get("route_status")) or fallback_status
        should_query_environment = bool(route.get("should_query_environment")) if maybe_text(route.get("route_status")) else fallback_env
        evidence_refs = unique_refs(
            list(
                cluster.get("evidence_refs", [])
                if isinstance(cluster.get("evidence_refs"), list)
                else (
                    cluster.get("public_refs", [])
                    if isinstance(cluster.get("public_refs"), list)
                    else []
                )
            )
            + list(scope.get("evidence_refs", []) if isinstance(scope.get("evidence_refs"), list) else [])
            + list(assessment.get("evidence_refs", []) if isinstance(assessment.get("evidence_refs"), list) else [])
            + list(route.get("evidence_refs", []) if isinstance(route.get("evidence_refs"), list) else [])
        )
        map_issue_id = "issuemap-" + stable_hash(run_id, round_id, claim_key, lane)[:12]
        claim_ids = list_field(cluster, "member_claim_ids") or ([claim_key] if claim_key else [])
        source_signal_ids = list_field(cluster, "source_signal_ids")
        issue_label = maybe_text(cluster.get("issue_label")) or maybe_text(cluster.get("cluster_label")) or maybe_text(scope.get("issue_hint")) or "general-public-controversy"
        issue_clusters.append(
            normalize_controversy_map_payload(
                {
                    "map_issue_id": map_issue_id,
                    "run_id": run_id,
                    "round_id": round_id,
                    "cluster_id": maybe_text(cluster.get("cluster_id")) or claim_key,
                    "claim_scope_id": maybe_text(scope.get("claim_scope_id")),
                    "assessment_id": maybe_text(assessment.get("assessment_id")),
                    "route_id": maybe_text(route.get("route_id")),
                    "claim_ids": claim_ids,
                    "source_signal_ids": source_signal_ids,
                    "issue_label": issue_label,
                    "claim_type": maybe_text(cluster.get("claim_type"))
                    or maybe_text(scope.get("claim_type")),
                    "dominant_stance": maybe_text(cluster.get("dominant_stance"))
                    or "unclear",
                    "stance_distribution": (
                        cluster.get("stance_distribution")
                        if isinstance(cluster.get("stance_distribution"), list)
                        else []
                    ),
                    "concern_facets": unique_texts(
                        list_field(cluster, "concern_facets")
                        + list_field(scope, "concern_facets")
                        + list_field(assessment, "concern_facets")
                    )[:5],
                    "actor_hints": unique_texts(
                        list_field(cluster, "actor_hints")
                        + list_field(scope, "actor_hints")
                        + list_field(assessment, "actor_hints")
                    )[:5],
                    "evidence_citation_types": unique_texts(
                        list_field(cluster, "evidence_citation_types")
                        + list_field(scope, "evidence_citation_types")
                        + list_field(assessment, "evidence_citation_types")
                    )[:5],
                    "verifiability_kind": maybe_text(assessment.get("verifiability_kind"))
                    or maybe_text(cluster.get("verifiability_posture"))
                    or maybe_text(scope.get("verifiability_kind"))
                    or "mixed-public-claim",
                    "dispute_type": maybe_text(route.get("dispute_type"))
                    or maybe_text(assessment.get("dispute_type"))
                    or maybe_text(cluster.get("dispute_type"))
                    or maybe_text(scope.get("dispute_type"))
                    or "mixed-controversy",
                    "recommended_lane": lane,
                    "route_status": route_status,
                    "should_query_environment": should_query_environment,
                    "member_count": int(cluster.get("member_count") or 1),
                    "aggregate_source_signal_count": int(
                        cluster.get("aggregate_source_signal_count") or 0
                    ),
                    "controversy_posture": controversy_posture(route_status, lane),
                    "controversy_summary": maybe_text(cluster.get("controversy_summary"))
                    or issue_summary(cluster, lane, route_status),
                    "evidence_refs": evidence_refs,
                    "lineage": unique_texts(
                        list(
                            cluster.get("lineage", [])
                            if isinstance(cluster.get("lineage"), list)
                            else []
                        )
                        + list(
                            scope.get("lineage", [])
                            if isinstance(scope.get("lineage"), list)
                            else []
                        )
                        + list(
                            assessment.get("lineage", [])
                            if isinstance(assessment.get("lineage"), list)
                            else []
                        )
                        + list(
                            route.get("lineage", [])
                            if isinstance(route.get("lineage"), list)
                            else []
                        )
                        + claim_ids
                        + source_signal_ids
                    ),
                    "rationale": (
                        "Merged cluster, scope, verifiability, and route state into "
                        f"one controversy-map issue object for {issue_label}."
                    ),
                    "method": "controversy-map-materialization-v2",
                    "selection_mode": "cluster-first-with-routing-merge",
                    "provenance": {
                        "fallback_route_used": not bool(maybe_text(route.get("route_id"))),
                        "fallback_scope_used": not bool(maybe_text(scope.get("claim_scope_id"))),
                    },
                },
                source_skill=SKILL_NAME,
                artifact_path=str(output_file),
            )
        )

    lane_counts = count_labels(
        [maybe_text(item.get("recommended_lane")) for item in issue_clusters]
    )
    route_status_counts = count_labels(
        [maybe_text(item.get("route_status")) for item in issue_clusters]
    )
    controversy_posture_counts = count_labels(
        [maybe_text(item.get("controversy_posture")) for item in issue_clusters]
    )
    concern_counts = count_labels(
        [value for item in issue_clusters for value in list_field(item, "concern_facets")]
    )
    actor_counts = count_labels(
        [value for item in issue_clusters for value in list_field(item, "actor_hints")]
    )

    actionable_gaps: list[str] = []
    if int(lane_counts.get("formal-comment-and-policy-record", 0)) > 0:
        actionable_gaps.append(
            "Some issues are procedural and still need formal-comment / policy-record handling."
        )
    if int(lane_counts.get("public-discourse-analysis", 0)) > 0:
        actionable_gaps.append(
            "Some issues are representational and should stay in discourse analysis rather than observation matching."
        )
    if int(lane_counts.get("route-before-matching", 0)) > 0 or int(
        lane_counts.get("mixed-review", 0)
    ) > 0:
        actionable_gaps.append(
            "Some issues still need routing clarification before the next board cycle."
        )
    if not actionable_gaps and issue_clusters:
        actionable_gaps.append(
            "Current controversy map is coherent enough to support next-action planning."
        )

    cluster_input_file = Path(maybe_text(cluster_context.get("claim_cluster_file")))
    generated_at_utc = utc_now_iso()
    issue_clusters_file = (run_dir_path / f"analytics/issue_clusters_{round_id}.json").resolve()
    stance_groups_file = (run_dir_path / f"analytics/stance_groups_{round_id}.json").resolve()
    concern_facets_file = (
        run_dir_path / f"analytics/concern_facets_{round_id}.json"
    ).resolve()
    actor_profiles_file = (run_dir_path / f"analytics/actor_profiles_{round_id}.json").resolve()
    evidence_citation_types_file = (
        run_dir_path / f"analytics/evidence_citation_types_{round_id}.json"
    ).resolve()
    typed_surfaces = build_typed_issue_surfaces(
        issue_clusters,
        run_id=run_id,
        round_id=round_id,
        issue_clusters_file=issue_clusters_file,
        stance_groups_file=stance_groups_file,
        concern_facets_file=concern_facets_file,
        actor_profiles_file=actor_profiles_file,
        evidence_citation_types_file=evidence_citation_types_file,
    )
    typed_issue_clusters = typed_surfaces["issue_clusters"]
    stance_groups = typed_surfaces["stance_groups"]
    concern_facets = typed_surfaces["concern_facets"]
    actor_profiles = typed_surfaces["actor_profiles"]
    citation_types = typed_surfaces["citation_types"]
    issue_label_counts = count_labels(
        [maybe_text(item.get("issue_label")) for item in typed_issue_clusters]
    )
    stance_label_counts = count_labels(
        [maybe_text(item.get("stance_label")) for item in stance_groups]
    )
    concern_label_counts = count_labels(
        [maybe_text(item.get("concern_label")) for item in concern_facets]
    )
    actor_label_counts = count_labels(
        [
            maybe_text(item.get("actor_label")) or maybe_text(item.get("display_name"))
            for item in actor_profiles
        ]
    )
    citation_type_counts = count_labels(
        [maybe_text(item.get("citation_type")) for item in citation_types]
    )

    wrapper = {
        "schema_version": "n3.0",
        "skill": SKILL_NAME,
        "generated_at_utc": generated_at_utc,
        "run_id": run_id,
        "round_id": round_id,
        "query_basis": {
            "cluster_input_path": str(cluster_input_file),
            "claim_scope_path": maybe_text(scope_context.get("claim_scope_file")),
            "verifiability_path": maybe_text(
                verifiability_context.get("claim_verifiability_file")
            ),
            "route_path": maybe_text(route_context.get("verification_route_file")),
            "cluster_source": maybe_text(cluster_context.get("claim_cluster_source"))
            or "missing-claim-cluster",
            "claim_scope_source": maybe_text(scope_context.get("claim_scope_source"))
            or "missing-claim-scope",
            "verifiability_source": maybe_text(
                verifiability_context.get("claim_verifiability_source")
            )
            or "missing-claim-verifiability",
            "route_source": maybe_text(route_context.get("verification_route_source"))
            or "missing-verification-route",
            "selection_mode": "cluster-first-with-routing-merge",
            "method": "controversy-map-materialization-v2",
        },
        "cluster_input_path": str(cluster_input_file),
        "claim_scope_path": maybe_text(scope_context.get("claim_scope_file")),
        "verifiability_path": maybe_text(
            verifiability_context.get("claim_verifiability_file")
        ),
        "route_path": maybe_text(route_context.get("verification_route_file")),
        "cluster_source": maybe_text(cluster_context.get("claim_cluster_source"))
        or "missing-claim-cluster",
        "claim_scope_source": maybe_text(scope_context.get("claim_scope_source"))
        or "missing-claim-scope",
        "verifiability_source": maybe_text(
            verifiability_context.get("claim_verifiability_source")
        )
        or "missing-claim-verifiability",
        "route_source": maybe_text(route_context.get("verification_route_source"))
        or "missing-verification-route",
        "observed_inputs": {
            "claim_clusters_present": bool(
                source_available(cluster_context.get("claim_cluster_source"))
            )
            or bool(claim_clusters),
            "claim_clusters_artifact_present": bool(
                cluster_context.get("claim_cluster_artifact_present")
            ),
            "claim_scope_present": bool(
                source_available(scope_context.get("claim_scope_source"))
            ),
            "claim_scope_artifact_present": bool(
                scope_context.get("claim_scope_artifact_present")
            ),
            "claim_verifiability_present": bool(
                source_available(
                    verifiability_context.get("claim_verifiability_source")
                )
            ),
            "claim_verifiability_artifact_present": bool(
                verifiability_context.get("claim_verifiability_artifact_present")
            ),
            "verification_route_present": bool(
                source_available(route_context.get("verification_route_source"))
            ),
            "verification_route_artifact_present": bool(
                route_context.get("verification_route_artifact_present")
            ),
        },
        "input_analysis_sync": {
            "claim_clusters": cluster_context.get("analysis_sync", {}),
            "claim_scope": scope_context.get("analysis_sync", {}),
            "claim_verifiability": verifiability_context.get("analysis_sync", {}),
            "verification_route": route_context.get("analysis_sync", {}),
        },
        "issue_cluster_count": len(issue_clusters),
        "lane_counts": lane_counts,
        "route_status_counts": route_status_counts,
        "controversy_posture_counts": controversy_posture_counts,
        "concern_counts": concern_counts,
        "actor_counts": actor_counts,
        "actionable_gaps": actionable_gaps,
        "typed_output_paths": {
            "issue_clusters_path": str(issue_clusters_file),
            "stance_groups_path": str(stance_groups_file),
            "concern_facets_path": str(concern_facets_file),
            "actor_profiles_path": str(actor_profiles_file),
            "evidence_citation_types_path": str(evidence_citation_types_file),
        },
        "typed_counts": {
            "issue_cluster_object_count": len(typed_issue_clusters),
            "stance_group_count": len(stance_groups),
            "concern_facet_count": len(concern_facets),
            "actor_profile_count": len(actor_profiles),
            "citation_type_count": len(citation_types),
        },
        "issue_clusters": issue_clusters,
    }
    analysis_db_path = (
        maybe_text(route_context.get("db_path"))
        or maybe_text(verifiability_context.get("db_path"))
        or maybe_text(scope_context.get("db_path"))
        or maybe_text(cluster_context.get("db_path"))
    )
    analysis_sync = sync_result_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        output_file=output_file,
        wrapper=wrapper,
        db_path=analysis_db_path,
        sync_callable=sync_controversy_map_result_set,
        path_kwarg="controversy_map_path",
    )
    analysis_db_path = maybe_text(analysis_sync.get("db_path")) or analysis_db_path

    issue_cluster_wrapper = {
        "schema_version": "n3.0",
        "skill": SKILL_NAME,
        "generated_at_utc": generated_at_utc,
        "run_id": run_id,
        "round_id": round_id,
        "query_basis": {
            "controversy_map_path": str(output_file),
            "controversy_map_source": SKILL_NAME,
            "selection_mode": "derive-issue-cluster-from-controversy-map",
            "method": "controversy-typed-decomposition-v1",
        },
        "controversy_map_path": str(output_file),
        "controversy_map_source": SKILL_NAME,
        "issue_label_counts": issue_label_counts,
        "issue_cluster_count": len(typed_issue_clusters),
        "issue_clusters": typed_issue_clusters,
    }
    issue_cluster_analysis_sync = sync_result_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        output_file=issue_clusters_file,
        wrapper=issue_cluster_wrapper,
        db_path=analysis_db_path,
        sync_callable=sync_issue_cluster_result_set,
        path_kwarg="issue_clusters_path",
    )
    analysis_db_path = (
        maybe_text(issue_cluster_analysis_sync.get("db_path")) or analysis_db_path
    )

    stance_group_wrapper = {
        "schema_version": "n3.0",
        "skill": SKILL_NAME,
        "generated_at_utc": generated_at_utc,
        "run_id": run_id,
        "round_id": round_id,
        "query_basis": {
            "controversy_map_path": str(output_file),
            "issue_clusters_path": str(issue_clusters_file),
            "controversy_map_source": SKILL_NAME,
            "issue_clusters_source": SKILL_NAME,
            "selection_mode": "derive-stance-groups-from-controversy-map",
            "method": "controversy-typed-decomposition-v1",
        },
        "controversy_map_path": str(output_file),
        "issue_clusters_path": str(issue_clusters_file),
        "controversy_map_source": SKILL_NAME,
        "issue_clusters_source": SKILL_NAME,
        "stance_label_counts": stance_label_counts,
        "stance_group_count": len(stance_groups),
        "stance_groups": stance_groups,
    }
    stance_group_analysis_sync = sync_result_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        output_file=stance_groups_file,
        wrapper=stance_group_wrapper,
        db_path=analysis_db_path,
        sync_callable=sync_stance_group_result_set,
        path_kwarg="stance_groups_path",
    )
    analysis_db_path = (
        maybe_text(stance_group_analysis_sync.get("db_path")) or analysis_db_path
    )

    concern_facet_wrapper = {
        "schema_version": "n3.0",
        "skill": SKILL_NAME,
        "generated_at_utc": generated_at_utc,
        "run_id": run_id,
        "round_id": round_id,
        "query_basis": {
            "controversy_map_path": str(output_file),
            "issue_clusters_path": str(issue_clusters_file),
            "controversy_map_source": SKILL_NAME,
            "issue_clusters_source": SKILL_NAME,
            "selection_mode": "derive-concern-facets-from-controversy-map",
            "method": "controversy-typed-decomposition-v1",
        },
        "controversy_map_path": str(output_file),
        "issue_clusters_path": str(issue_clusters_file),
        "controversy_map_source": SKILL_NAME,
        "issue_clusters_source": SKILL_NAME,
        "concern_label_counts": concern_label_counts,
        "concern_facet_count": len(concern_facets),
        "concern_facets": concern_facets,
    }
    concern_facet_analysis_sync = sync_result_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        output_file=concern_facets_file,
        wrapper=concern_facet_wrapper,
        db_path=analysis_db_path,
        sync_callable=sync_concern_facet_result_set,
        path_kwarg="concern_facets_path",
    )
    analysis_db_path = (
        maybe_text(concern_facet_analysis_sync.get("db_path")) or analysis_db_path
    )

    actor_profile_wrapper = {
        "schema_version": "n3.0",
        "skill": SKILL_NAME,
        "generated_at_utc": generated_at_utc,
        "run_id": run_id,
        "round_id": round_id,
        "query_basis": {
            "controversy_map_path": str(output_file),
            "issue_clusters_path": str(issue_clusters_file),
            "controversy_map_source": SKILL_NAME,
            "issue_clusters_source": SKILL_NAME,
            "selection_mode": "derive-actor-profiles-from-controversy-map",
            "method": "controversy-typed-decomposition-v1",
        },
        "controversy_map_path": str(output_file),
        "issue_clusters_path": str(issue_clusters_file),
        "controversy_map_source": SKILL_NAME,
        "issue_clusters_source": SKILL_NAME,
        "actor_label_counts": actor_label_counts,
        "actor_profile_count": len(actor_profiles),
        "actor_profiles": actor_profiles,
    }
    actor_profile_analysis_sync = sync_result_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        output_file=actor_profiles_file,
        wrapper=actor_profile_wrapper,
        db_path=analysis_db_path,
        sync_callable=sync_actor_profile_result_set,
        path_kwarg="actor_profiles_path",
    )
    analysis_db_path = (
        maybe_text(actor_profile_analysis_sync.get("db_path")) or analysis_db_path
    )

    evidence_citation_type_wrapper = {
        "schema_version": "n3.0",
        "skill": SKILL_NAME,
        "generated_at_utc": generated_at_utc,
        "run_id": run_id,
        "round_id": round_id,
        "query_basis": {
            "controversy_map_path": str(output_file),
            "issue_clusters_path": str(issue_clusters_file),
            "controversy_map_source": SKILL_NAME,
            "issue_clusters_source": SKILL_NAME,
            "selection_mode": "derive-citation-types-from-controversy-map",
            "method": "controversy-typed-decomposition-v1",
        },
        "controversy_map_path": str(output_file),
        "issue_clusters_path": str(issue_clusters_file),
        "controversy_map_source": SKILL_NAME,
        "issue_clusters_source": SKILL_NAME,
        "citation_type_counts": citation_type_counts,
        "citation_type_count": len(citation_types),
        "citation_types": citation_types,
    }
    evidence_citation_type_analysis_sync = sync_result_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        output_file=evidence_citation_types_file,
        wrapper=evidence_citation_type_wrapper,
        db_path=analysis_db_path,
        sync_callable=sync_evidence_citation_type_result_set,
        path_kwarg="evidence_citation_types_path",
    )
    analysis_db_path = (
        maybe_text(evidence_citation_type_analysis_sync.get("db_path"))
        or analysis_db_path
    )

    issue_cluster_analysis_sync = sync_issue_cluster_result_set(
        run_dir_path,
        expected_run_id=run_id,
        round_id=round_id,
        issue_clusters_path=issue_clusters_file,
        db_path=analysis_db_path,
    )
    issue_cluster_wrapper["db_path"] = maybe_text(issue_cluster_analysis_sync.get("db_path"))
    issue_cluster_wrapper["analysis_sync"] = issue_cluster_analysis_sync
    write_json(issue_clusters_file, issue_cluster_wrapper)
    stance_group_analysis_sync = sync_stance_group_result_set(
        run_dir_path,
        expected_run_id=run_id,
        round_id=round_id,
        stance_groups_path=stance_groups_file,
        db_path=analysis_db_path,
    )
    stance_group_wrapper["db_path"] = maybe_text(stance_group_analysis_sync.get("db_path"))
    stance_group_wrapper["analysis_sync"] = stance_group_analysis_sync
    write_json(stance_groups_file, stance_group_wrapper)
    concern_facet_analysis_sync = sync_concern_facet_result_set(
        run_dir_path,
        expected_run_id=run_id,
        round_id=round_id,
        concern_facets_path=concern_facets_file,
        db_path=analysis_db_path,
    )
    concern_facet_wrapper["db_path"] = maybe_text(concern_facet_analysis_sync.get("db_path"))
    concern_facet_wrapper["analysis_sync"] = concern_facet_analysis_sync
    write_json(concern_facets_file, concern_facet_wrapper)
    actor_profile_analysis_sync = sync_actor_profile_result_set(
        run_dir_path,
        expected_run_id=run_id,
        round_id=round_id,
        actor_profiles_path=actor_profiles_file,
        db_path=analysis_db_path,
    )
    actor_profile_wrapper["db_path"] = maybe_text(actor_profile_analysis_sync.get("db_path"))
    actor_profile_wrapper["analysis_sync"] = actor_profile_analysis_sync
    write_json(actor_profiles_file, actor_profile_wrapper)
    evidence_citation_type_analysis_sync = sync_evidence_citation_type_result_set(
        run_dir_path,
        expected_run_id=run_id,
        round_id=round_id,
        evidence_citation_types_path=evidence_citation_types_file,
        db_path=analysis_db_path,
    )
    evidence_citation_type_wrapper["db_path"] = maybe_text(
        evidence_citation_type_analysis_sync.get("db_path")
    )
    evidence_citation_type_wrapper["analysis_sync"] = (
        evidence_citation_type_analysis_sync
    )
    write_json(evidence_citation_types_file, evidence_citation_type_wrapper)

    typed_analysis_sync = {
        "issue-cluster": issue_cluster_analysis_sync,
        "stance-group": stance_group_analysis_sync,
        "concern-facet": concern_facet_analysis_sync,
        "actor-profile": actor_profile_analysis_sync,
        "evidence-citation-type": evidence_citation_type_analysis_sync,
    }
    wrapper["db_path"] = analysis_db_path
    wrapper["typed_analysis_sync"] = typed_analysis_sync
    write_json(output_file, wrapper)

    artifact_refs = [
        artifact_ref_for_path(output_file, "$.issue_clusters"),
        artifact_ref_for_path(issue_clusters_file, "$.issue_clusters"),
        artifact_ref_for_path(stance_groups_file, "$.stance_groups"),
        artifact_ref_for_path(concern_facets_file, "$.concern_facets"),
        artifact_ref_for_path(actor_profiles_file, "$.actor_profiles"),
        artifact_ref_for_path(evidence_citation_types_file, "$.citation_types"),
    ]
    canonical_ids = [
        maybe_text(item.get("cluster_id"))
        for item in typed_issue_clusters
        if maybe_text(item.get("cluster_id"))
    ]
    if not canonical_ids:
        canonical_ids = [
            maybe_text(item.get("map_issue_id"))
            for item in issue_clusters
            if maybe_text(item.get("map_issue_id"))
        ]
    if not issue_clusters:
        warnings.append(
            {
                "code": "no-controversy-map",
                "message": "No controversy-map issue clusters were produced from the available claim-side inputs.",
            }
        )
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "issue_cluster_count": len(issue_clusters),
            "typed_issue_cluster_count": len(typed_issue_clusters),
            "stance_group_count": len(stance_groups),
            "concern_facet_count": len(concern_facets),
            "actor_profile_count": len(actor_profiles),
            "citation_type_count": len(citation_types),
            "cluster_source": wrapper["cluster_source"],
            "route_source": wrapper["route_source"],
            "db_path": analysis_db_path,
        },
        "receipt_id": "controversy-map-receipt-"
        + stable_hash(SKILL_NAME, run_id, round_id, str(output_file))[:20],
        "batch_id": "controversymapbatch-"
        + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": canonical_ids,
        "warnings": warnings,
        "analysis_sync": analysis_sync,
        "typed_analysis_sync": typed_analysis_sync,
        "input_analysis_sync": wrapper.get("input_analysis_sync", {}),
        "board_handoff": {
            "candidate_ids": canonical_ids,
            "evidence_refs": artifact_refs[:2],
            "gap_hints": actionable_gaps[:3],
            "challenge_hints": (
                ["Review whether any issue still routed to observation matching is actually a procedural controversy."]
                if int(controversy_posture_counts.get("empirical-issue", 0)) > 0
                and int(controversy_posture_counts.get("non-empirical-issue", 0)) > 0
                else []
            ),
            "suggested_next_skills": [
                "eco-propose-next-actions",
                "eco-post-board-note",
                "eco-open-falsification-probe",
            ],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Materialize a compact controversy map from the current claim-side chain."
    )
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--claim-cluster-path", default="")
    parser.add_argument("--claim-scope-path", default="")
    parser.add_argument("--claim-verifiability-path", default="")
    parser.add_argument("--verification-route-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = materialize_controversy_map_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        claim_cluster_path=args.claim_cluster_path,
        claim_scope_path=args.claim_scope_path,
        claim_verifiability_path=args.claim_verifiability_path,
        verification_route_path=args.verification_route_path,
        output_path=args.output_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
