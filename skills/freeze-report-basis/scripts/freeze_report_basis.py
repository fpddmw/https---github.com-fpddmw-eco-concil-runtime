#!/usr/bin/env python3
"""Freeze round evidence into a compact report-basis artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "freeze-report-basis"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.phase2_fallback_context import load_d1_shared_context  # noqa: E402
from eco_council_runtime.phase2_report_basis_resolution import (  # noqa: E402
    load_council_proposals,
    load_council_readiness_opinions,
    resolve_report_basis_council_inputs,
)
from eco_council_runtime.phase2_action_semantics import (  # noqa: E402
    action_is_readiness_blocker,
)
from eco_council_runtime.kernel.phase2_state_surfaces import (  # noqa: E402
    load_next_actions_wrapper,
    load_round_readiness_wrapper,
)
from eco_council_runtime.kernel.deliberation_plane import (  # noqa: E402
    store_report_basis_freeze_record,
)
from eco_council_runtime.kernel.reporting_contracts import (  # noqa: E402
    reporting_contract_fields_from_payload,
)
from eco_council_runtime.kernel.transition_requests import (  # noqa: E402
    TRANSITION_KIND_FREEZE_REPORT_BASIS,
    mark_transition_request_committed,
    request_payload_option,
    resolve_transition_request_for_execution,
)


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def maybe_number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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


def list_items(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


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


def resolve_path(run_dir: Path, override: str, default_relative: str) -> Path:
    text = maybe_text(override)
    if not text:
        return (run_dir / default_relative).resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def load_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return payload
    return None


def load_text_if_exists(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def list_field(item: dict[str, Any], key: str) -> list[str]:
    values = item.get(key)
    if not isinstance(values, list):
        return []
    return [maybe_text(value) for value in values if maybe_text(value)]


def artifact_ref_text(value: Any) -> str:
    if isinstance(value, dict):
        artifact_ref = maybe_text(value.get("artifact_ref"))
        if artifact_ref:
            return artifact_ref
        artifact_path = maybe_text(value.get("artifact_path"))
        record_locator = maybe_text(value.get("record_locator"))
        if artifact_path and record_locator:
            return f"{artifact_path}:{record_locator}"
        return artifact_path
    return maybe_text(value)


def unique_artifact_ref_texts(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    results: list[str] = []
    for value in values:
        text = artifact_ref_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        results.append(text)
    return results


def normalize_issue_cluster(item: dict[str, Any]) -> dict[str, Any]:
    schema_version = maybe_text(item.get("schema_version"))
    prefers_issue_cluster_id = schema_version.startswith("issue-cluster-")
    object_id = (
        maybe_text(item.get("cluster_id"))
        if prefers_issue_cluster_id
        else (
            maybe_text(item.get("map_issue_id"))
            or maybe_text(item.get("cluster_id"))
        )
    )
    return {
        "object_type": "issue-cluster",
        "object_id": object_id,
        "issue_label": maybe_text(item.get("issue_label")),
        "cluster_id": maybe_text(item.get("cluster_id")),
        "map_issue_id": maybe_text(item.get("map_issue_id")) or object_id,
        "claim_cluster_id": maybe_text(item.get("claim_cluster_id")),
        "claim_ids": list_field(item, "claim_ids"),
        "dominant_stance": maybe_text(item.get("dominant_stance")),
        "controversy_posture": maybe_text(item.get("controversy_posture")),
        "recommended_lane": maybe_text(item.get("recommended_lane")),
        "route_status": maybe_text(item.get("route_status")),
        "concern_facets": list_field(item, "concern_facets"),
        "actor_hints": list_field(item, "actor_hints"),
        "stance_group_ids": list_field(item, "stance_group_ids"),
        "concern_ids": list_field(item, "concern_ids"),
        "actor_ids": list_field(item, "actor_ids"),
        "citation_type_ids": list_field(item, "citation_type_ids"),
        "summary": maybe_text(item.get("issue_summary"))
        or maybe_text(item.get("controversy_summary")),
        "evidence_refs": unique_artifact_ref_texts(
            item.get("evidence_refs", [])
            if isinstance(item.get("evidence_refs"), list)
            else []
        ),
    }


def normalize_verification_route(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "object_type": "verification-route",
        "object_id": maybe_text(item.get("route_id")),
        "issue_label": maybe_text(item.get("issue_hint")) or maybe_text(item.get("claim_id")),
        "claim_id": maybe_text(item.get("claim_id")),
        "recommended_lane": maybe_text(item.get("recommended_lane")),
        "route_status": maybe_text(item.get("route_status")),
        "verifiability_kind": maybe_text(item.get("verifiability_kind")),
        "summary": maybe_text(item.get("route_reason")),
        "evidence_refs": unique_artifact_ref_texts(
            item.get("evidence_refs", [])
            if isinstance(item.get("evidence_refs"), list)
            else []
        ),
    }


def normalize_formal_public_link(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "object_type": "formal-public-link",
        "object_id": maybe_text(item.get("linkage_id")),
        "issue_label": maybe_text(item.get("issue_label")),
        "claim_ids": list_field(item, "claim_ids"),
        "cluster_ids": list_field(item, "cluster_ids"),
        "link_status": maybe_text(item.get("link_status")),
        "recommended_lane": maybe_text(item.get("recommended_lane")),
        "route_status": maybe_text(item.get("route_status")),
        "formal_signal_count": int(item.get("formal_signal_count") or 0),
        "public_signal_count": int(item.get("public_signal_count") or 0),
        "summary": maybe_text(item.get("linkage_summary")),
        "evidence_refs": unique_artifact_ref_texts(
            item.get("evidence_refs", [])
            if isinstance(item.get("evidence_refs"), list)
            else []
        ),
    }


def normalize_representation_gap(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "object_type": "representation-gap",
        "object_id": maybe_text(item.get("gap_id")),
        "issue_label": maybe_text(item.get("issue_label")),
        "claim_ids": list_field(item, "claim_ids"),
        "cluster_ids": list_field(item, "cluster_ids"),
        "gap_type": maybe_text(item.get("gap_type")),
        "severity": maybe_text(item.get("severity")),
        "severity_score": float(item.get("severity_score") or 0.0),
        "recommended_lane": maybe_text(item.get("recommended_lane")),
        "route_status": maybe_text(item.get("route_status")),
        "summary": maybe_text(item.get("gap_summary")) or maybe_text(item.get("recommended_action")),
        "evidence_refs": unique_artifact_ref_texts(
            item.get("evidence_refs", [])
            if isinstance(item.get("evidence_refs"), list)
            else []
        ),
    }


def normalize_diffusion_edge(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "object_type": "diffusion-edge",
        "object_id": maybe_text(item.get("edge_id")),
        "issue_label": maybe_text(item.get("issue_label")),
        "claim_ids": list_field(item, "claim_ids"),
        "cluster_ids": list_field(item, "cluster_ids"),
        "edge_type": maybe_text(item.get("edge_type")),
        "confidence": float(item.get("confidence") or 0.0),
        "recommended_lane": maybe_text(item.get("recommended_lane")),
        "route_status": maybe_text(item.get("route_status")),
        "source_platform": maybe_text(item.get("source_platform")),
        "target_platform": maybe_text(item.get("target_platform")),
        "summary": maybe_text(item.get("edge_summary")),
        "evidence_refs": unique_artifact_ref_texts(
            item.get("evidence_refs", [])
            if isinstance(item.get("evidence_refs"), list)
            else []
        ),
    }


def selected_issue_clusters(issue_clusters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [
        normalize_issue_cluster(item)
        for item in issue_clusters
        if isinstance(item, dict)
    ]
    return sorted(
        [row for row in rows if maybe_text(row.get("object_id"))],
        key=lambda row: (maybe_text(row.get("issue_label")), maybe_text(row.get("object_id"))),
    )


def selected_verification_routes(routes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in routes:
        if not isinstance(item, dict):
            continue
        normalized = normalize_verification_route(item)
        if maybe_text(normalized.get("object_id")):
            rows.append(normalized)
    return sorted(
        rows,
        key=lambda row: (maybe_text(row.get("issue_label")), maybe_text(row.get("object_id"))),
    )


def selected_formal_public_links(links: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [
        normalize_formal_public_link(item)
        for item in links
        if isinstance(item, dict)
        and maybe_text(item.get("link_status")) not in {"", "aligned"}
    ]
    return sorted(
        [row for row in rows if maybe_text(row.get("object_id"))],
        key=lambda row: (maybe_text(row.get("issue_label")), maybe_text(row.get("object_id"))),
    )


def selected_representation_gaps(gaps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [
        normalize_representation_gap(item)
        for item in gaps
        if isinstance(item, dict)
    ]
    return sorted(
        [row for row in rows if maybe_text(row.get("object_id"))],
        key=lambda row: (
            -(maybe_number(row.get("severity_score")) or 0.0),
            maybe_text(row.get("issue_label")),
            maybe_text(row.get("object_id")),
        ),
    )


def selected_diffusion_edges(edges: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in edges:
        if not isinstance(item, dict):
            continue
        edge_type = maybe_text(item.get("edge_type"))
        confidence = maybe_number(item.get("confidence")) or 0.0
        if edge_type not in {
            "public-to-formal-spillover",
            "formal-to-public-spillover",
            "cross-public-diffusion",
        }:
            continue
        if confidence < 0.72:
            continue
        normalized = normalize_diffusion_edge(item)
        if maybe_text(normalized.get("object_id")):
            rows.append(normalized)
    return sorted(
        rows,
        key=lambda row: (
            -(maybe_number(row.get("confidence")) or 0.0),
            maybe_text(row.get("issue_label")),
            maybe_text(row.get("object_id")),
        ),
    )


def route_is_explicit_empirical(item: dict[str, Any]) -> bool:
    lane = maybe_text(item.get("recommended_lane"))
    route_status = maybe_text(item.get("route_status")) or "mixed-routing-review"
    return (
        lane == "environmental-observation"
        and route_status != "mixed-routing-review"
    )


def empirical_lane_claim_ids(
    issue_clusters: list[dict[str, Any]],
    routes: list[dict[str, Any]],
) -> list[str]:
    claim_ids: list[str] = []
    for issue in issue_clusters:
        if not isinstance(issue, dict) or not route_is_explicit_empirical(issue):
            continue
        claim_ids.extend(list_field(issue, "claim_ids"))
    for route in routes:
        if not isinstance(route, dict) or not route_is_explicit_empirical(route):
            continue
        claim_id = maybe_text(route.get("claim_id"))
        if claim_id:
            claim_ids.append(claim_id)
    return unique_texts(claim_ids)


def selected_empirical_support_coverages(
    coverages: list[dict[str, Any]],
    *,
    empirical_claim_ids: list[str],
    max_coverages: int,
) -> list[dict[str, Any]]:
    if not empirical_claim_ids:
        return []
    allowed_claim_ids = set(empirical_claim_ids)
    ranked_coverages = [
        item
        for item in coverages
        if isinstance(item, dict)
        and maybe_text(item.get("claim_id")) in allowed_claim_ids
    ]
    return selected_support_coverages(
        ranked_coverages,
        max_coverages=max_coverages,
    )


def selected_support_coverages(
    coverages: list[dict[str, Any]],
    *,
    max_coverages: int,
) -> list[dict[str, Any]]:
    ranked_coverages = sorted(
        [item for item in coverages if isinstance(item, dict)],
        key=lambda item: (
            -float(item.get("coverage_score") or 0.0),
            maybe_text(item.get("coverage_id")),
        ),
    )[: max(1, max_coverages)]
    return [
        {
            "coverage_id": maybe_text(item.get("coverage_id")),
            "claim_id": maybe_text(item.get("claim_id")),
            "coverage_score": float(item.get("coverage_score") or 0.0),
            "readiness": maybe_text(item.get("readiness")),
            "support_link_count": int(item.get("support_link_count") or 0),
            "contradiction_link_count": int(item.get("contradiction_link_count") or 0),
            "evidence_refs": unique_artifact_ref_texts(
                item.get("evidence_refs", [])
                if isinstance(item.get("evidence_refs"), list)
                else []
            ),
        }
        for item in ranked_coverages
    ]


def basis_object_ids(
    frozen_basis: dict[str, Any],
    selected_coverages: list[dict[str, Any]],
) -> list[str]:
    ids: list[Any] = []
    for key in (
        "issue_clusters",
        "verification_routes",
        "formal_public_links",
        "representation_gaps",
        "diffusion_edges",
    ):
        rows = frozen_basis.get(key, [])
        if not isinstance(rows, list):
            continue
        ids.extend(
            row.get("object_id")
            for row in rows
            if isinstance(row, dict)
        )
    ids.extend(
        coverage.get("coverage_id")
        for coverage in selected_coverages
        if isinstance(coverage, dict)
    )
    return unique_texts(ids)


def freeze_report_basis_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    transition_request_id: str,
    readiness_path: str,
    board_brief_path: str,
    coverage_path: str,
    next_actions_path: str,
    output_path: str,
    allow_non_ready: bool,
    max_coverages: int,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    transition_request = resolve_transition_request_for_execution(
        run_dir_path,
        request_id=transition_request_id,
        transition_kind=TRANSITION_KIND_FREEZE_REPORT_BASIS,
        run_id=run_id,
        round_id=round_id,
    )
    if not allow_non_ready:
        allow_non_ready = bool(
            request_payload_option(transition_request, "allow_non_ready", False)
        )
    requested_max_coverages = request_payload_option(
        transition_request,
        "max_coverages",
        max_coverages,
    )
    try:
        max_coverages = max(1, int(requested_max_coverages or max_coverages))
    except (TypeError, ValueError):
        max_coverages = max(1, int(max_coverages or 1))
    readiness_file = resolve_path(run_dir_path, readiness_path, f"reporting/round_readiness_{round_id}.json")
    board_brief_file = resolve_path(run_dir_path, board_brief_path, f"board/board_brief_{round_id}.md")
    next_actions_file = resolve_path(run_dir_path, next_actions_path, f"investigation/next_actions_{round_id}.json")
    output_file = resolve_path(run_dir_path, output_path, f"report_basis/frozen_report_basis_{round_id}.json")

    warnings: list[dict[str, Any]] = []
    readiness_context = load_round_readiness_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        readiness_path=readiness_path,
    )
    readiness_payload = (
        readiness_context.get("payload")
        if isinstance(readiness_context.get("payload"), dict)
        else None
    )
    if not isinstance(readiness_payload, dict):
        warnings.append(
            {
                "code": "missing-readiness",
                "message": (
                    "No round readiness DB assessment was found for "
                    f"{readiness_file}; artifact exists but is orphaned from the deliberation plane."
                    if bool(readiness_context.get("artifact_present"))
                    else (
                        "No round readiness artifact or DB assessment was found "
                        f"at {readiness_file}."
                    )
                ),
            }
        )
        readiness = {
            "readiness_status": "blocked",
            "gate_reasons": ["Missing round readiness artifact or DB assessment."],
            "counts": {},
            "recommended_next_skills": [],
        }
    else:
        readiness = readiness_payload
    shared_context = load_d1_shared_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        board_brief_path=board_brief_path,
        coverage_path=coverage_path,
    )
    shared_warnings = (
        shared_context.get("warnings", [])
        if isinstance(shared_context.get("warnings"), list)
        else []
    )
    warnings.extend(shared_warnings)
    coverages = (
        shared_context.get("coverages", [])
        if isinstance(shared_context.get("coverages"), list)
        else []
    )
    issue_clusters = (
        shared_context.get("issue_clusters", [])
        if isinstance(shared_context.get("issue_clusters"), list)
        else []
    )
    routes = (
        shared_context.get("routes", [])
        if isinstance(shared_context.get("routes"), list)
        else []
    )
    links = (
        shared_context.get("formal_public_links", [])
        if isinstance(shared_context.get("formal_public_links"), list)
        else []
    )
    gaps = (
        shared_context.get("representation_gaps", [])
        if isinstance(shared_context.get("representation_gaps"), list)
        else []
    )
    edges = (
        shared_context.get("diffusion_edges", [])
        if isinstance(shared_context.get("diffusion_edges"), list)
        else []
    )
    agenda_counts = (
        shared_context.get("agenda_counts")
        if isinstance(shared_context.get("agenda_counts"), dict)
        else {}
    )
    coverage_file = maybe_text(shared_context.get("coverage_file"))
    coverage_source = maybe_text(shared_context.get("coverage_source")) or "missing-coverage"
    db_path = maybe_text(shared_context.get("db_path"))
    analysis_sync = (
        shared_context.get("analysis_sync")
        if isinstance(shared_context.get("analysis_sync"), dict)
        else {}
    )
    next_actions_context = load_next_actions_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        next_actions_path=next_actions_path,
    )
    next_actions_payload = (
        next_actions_context.get("payload")
        if isinstance(next_actions_context.get("payload"), dict)
        else None
    )
    next_actions = next_actions_payload if isinstance(next_actions_payload, dict) else {"ranked_actions": []}
    brief_text = maybe_text(load_text_if_exists(board_brief_file))
    shared_observed_inputs = (
        shared_context.get("observed_inputs")
        if isinstance(shared_context.get("observed_inputs"), dict)
        else {}
    )
    contract_fields = reporting_contract_fields_from_payload(
        readiness_payload,
        observed_inputs_overrides={
            "readiness_artifact_present": bool(
                readiness_context.get("artifact_present")
            ),
            "readiness_present": bool(readiness_context.get("payload_present")),
            "board_brief_artifact_present": board_brief_file.exists(),
            "board_brief_present": bool(brief_text),
            "coverage_artifact_present": bool(
                shared_observed_inputs.get("coverage_artifact_present")
            ),
            "coverage_present": bool(coverages),
            "next_actions_artifact_present": bool(
                next_actions_context.get("artifact_present")
            ),
            "next_actions_present": bool(next_actions_context.get("payload_present")),
        },
        field_overrides={
            "coverage_source": coverage_source or "missing-coverage",
            "db_path": db_path,
            "readiness_source": maybe_text(readiness_context.get("source"))
            or "missing-readiness",
            "board_brief_source": (
                "board-brief-artifact"
                if board_brief_file.exists()
                else "missing-board-brief"
            ),
            "next_actions_source": maybe_text(next_actions_context.get("source"))
            or "missing-next-actions",
        },
    )

    readiness_status = maybe_text(readiness.get("readiness_status")) or "blocked"

    empirical_claim_ids = empirical_lane_claim_ids(issue_clusters, routes)
    selected_coverages = selected_empirical_support_coverages(
        coverages,
        empirical_claim_ids=empirical_claim_ids,
        max_coverages=max_coverages,
    )
    structural_basis_present = bool(
        issue_clusters
        or routes
        or links
        or gaps
        or edges
    )
    if not selected_coverages and not structural_basis_present:
        selected_coverages = selected_support_coverages(
            coverages,
            max_coverages=max_coverages,
        )
    frozen_basis = {
        "issue_clusters": selected_issue_clusters(issue_clusters),
        "verification_routes": selected_verification_routes(routes),
        "formal_public_links": selected_formal_public_links(links),
        "representation_gaps": selected_representation_gaps(gaps),
        "diffusion_edges": selected_diffusion_edges(edges),
        "empirical_support_coverages": selected_coverages,
        "coverages": selected_coverages,
    }
    basis_counts = {
        "issue_cluster_count": len(frozen_basis["issue_clusters"]),
        "verification_route_count": len(frozen_basis["verification_routes"]),
        "formal_public_link_count": len(frozen_basis["formal_public_links"]),
        "representation_gap_count": len(frozen_basis["representation_gaps"]),
        "diffusion_edge_count": len(frozen_basis["diffusion_edges"]),
        "empirical_support_coverage_count": len(selected_coverages),
        "coverage_count": len(selected_coverages),
    }
    selected_basis_object_ids = basis_object_ids(
        frozen_basis,
        selected_coverages,
    )
    council_proposals = load_council_proposals(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    council_opinions = load_council_readiness_opinions(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    report_basis_resolution = resolve_report_basis_council_inputs(
        council_proposals,
        council_opinions,
        readiness_status=readiness_status,
        allow_non_ready=allow_non_ready,
        round_id=round_id,
        selected_basis_object_ids=selected_basis_object_ids,
    )
    report_basis_status = (
        maybe_text(report_basis_resolution.get("report_basis_status")) or "withheld"
    )
    report_basis_resolution_reasons = (
        report_basis_resolution.get("report_basis_resolution_reasons", [])
        if isinstance(report_basis_resolution.get("report_basis_resolution_reasons"), list)
        else report_basis_resolution.get("report_basis_resolution_reasons", [])
        if isinstance(report_basis_resolution.get("report_basis_resolution_reasons"), list)
        else []
    )
    if report_basis_status == "withheld":
        warnings.append(
            {
                "code": "report-basis-withheld",
                "message": maybe_text(report_basis_resolution_reasons[0])
                or "Promotion was withheld because the round-readiness gate is not ready.",
            }
        )
    supporting_proposal_ids = unique_texts(
        report_basis_resolution.get("supporting_proposal_ids", [])
        if isinstance(report_basis_resolution.get("supporting_proposal_ids"), list)
        else []
    )
    rejected_proposal_ids = unique_texts(
        report_basis_resolution.get("rejected_proposal_ids", [])
        if isinstance(report_basis_resolution.get("rejected_proposal_ids"), list)
        else []
    )
    supporting_opinion_ids = unique_texts(
        report_basis_resolution.get("supporting_opinion_ids", [])
        if isinstance(report_basis_resolution.get("supporting_opinion_ids"), list)
        else []
    )
    rejected_opinion_ids = unique_texts(
        report_basis_resolution.get("rejected_opinion_ids", [])
        if isinstance(report_basis_resolution.get("rejected_opinion_ids"), list)
        else []
    )
    proposal_resolution_records = (
        report_basis_resolution.get("proposal_resolution_records", [])
        if isinstance(report_basis_resolution.get("proposal_resolution_records"), list)
        else []
    )
    ignored_implicit_report_basis_proposals = [
        record
        for record in proposal_resolution_records
        if isinstance(record, dict)
        and maybe_text(record.get("resolution_mode"))
        == "ignored-implicit-report-basis-operation"
    ]
    if ignored_implicit_report_basis_proposals:
        warnings.append(
            {
                "code": "ignored-implicit-report-basis-operation",
                "message": (
                    f"{len(ignored_implicit_report_basis_proposals)} council proposals "
                    "used legacy transition naming without explicit structured "
                    "judgement fields and were ignored for report-basis support."
                ),
            }
        )
    rejected_proposals = [
        proposal
        for proposal in council_proposals
        if maybe_text(proposal.get("proposal_id")) in rejected_proposal_ids
    ]
    rejected_opinions = [
        opinion
        for opinion in council_opinions
        if maybe_text(opinion.get("opinion_id")) in rejected_opinion_ids
    ]
    selected_evidence_refs = unique_artifact_ref_texts(
        [
            ref
            for item in selected_coverages
            for ref in item.get("evidence_refs", [])
        ]
        + [
            ref
            for key in (
                "issue_clusters",
                "verification_routes",
                "formal_public_links",
                "representation_gaps",
                "diffusion_edges",
            )
            for row in frozen_basis.get(key, [])
            if isinstance(row, dict)
            for ref in row.get("evidence_refs", [])
        ]
    )
    selected_evidence_refs = unique_artifact_ref_texts(
        selected_evidence_refs
        + [
            ref
            for opinion in council_opinions
            for ref in list_items(opinion.get("evidence_refs"))
        ]
        + [
            ref
            for proposal in council_proposals
            for ref in list_items(proposal.get("evidence_refs"))
        ]
    )
    fallback_remaining_risks = [
        {
            "action_id": maybe_text(item.get("action_id")),
            "action_kind": maybe_text(item.get("action_kind")),
            "priority": maybe_text(item.get("priority")),
            "reason": maybe_text(item.get("reason")),
            "controversy_gap": maybe_text(item.get("controversy_gap")),
            "recommended_lane": maybe_text(item.get("recommended_lane")),
        }
        for item in next_actions.get("ranked_actions", [])
        if isinstance(item, dict) and action_is_readiness_blocker(item)
    ][:4] if isinstance(next_actions.get("ranked_actions"), list) else []
    remaining_risks = (
        [
            {
                "action_id": maybe_text(proposal.get("proposal_id")),
                "action_kind": "proposal-veto",
                "priority": "high",
                "reason": maybe_text(proposal.get("rationale"))
                or "An open council proposal explicitly withholds report-basis freeze.",
                "controversy_gap": "council-report_basis-veto",
                "recommended_lane": "council-deliberation",
            }
            for proposal in rejected_proposals[:3]
        ]
        + [
            {
                "action_id": maybe_text(opinion.get("opinion_id")),
                "action_kind": "readiness-opinion",
                "priority": "high",
                "reason": maybe_text(opinion.get("rationale"))
                or "A council readiness opinion still blocks report-basis freeze.",
                "controversy_gap": "council-readiness-disagreement",
                "recommended_lane": "council-deliberation",
            }
            for opinion in rejected_opinions[:3]
        ]
        + fallback_remaining_risks
    )[:4]

    basis_id = "report-basis-" + stable_hash(run_id, round_id, report_basis_status)[:12]
    decision_source = maybe_text(readiness.get("decision_source")) or "policy-fallback"
    if maybe_text(report_basis_resolution.get("decision_source")):
        decision_source = maybe_text(report_basis_resolution.get("decision_source"))
    merged_gate_reasons = unique_texts(
        report_basis_resolution_reasons
        + (
            readiness.get("gate_reasons", [])
            if isinstance(readiness.get("gate_reasons"), list)
            else []
        )
    )
    lineage = unique_texts(
        selected_basis_object_ids
        + [
            coverage.get("coverage_id")
            for coverage in selected_coverages
            if isinstance(coverage, dict)
        ]
        + supporting_proposal_ids
        + rejected_proposal_ids
        + supporting_opinion_ids
        + rejected_opinion_ids
    )
    wrapper = {
        "schema_version": "d2.1",
        "skill": SKILL_NAME,
        "basis_object_kind": "report-basis-freeze",
        "transition_semantics": "freeze-report-basis",
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "basis_id": basis_id,
        "report_basis_status": report_basis_status,
        "transition_request_id": maybe_text(transition_request.get("request_id")),
        "readiness_status": readiness_status,
        "decision_source": decision_source,
        "readiness_path": str(readiness_file),
        "board_brief_path": str(board_brief_file),
        "coverage_path": str(coverage_file),
        **contract_fields,
        "agenda_counts": agenda_counts,
        "basis_selection_mode": (
            "council-judgement-freeze-v1"
            if decision_source == "agent-council"
            else "freeze-controversy-basis-v1"
        ),
        "report_basis_selection_mode": "freeze-report-basis-v1",
        "report_basis_resolution_mode": maybe_text(
            report_basis_resolution.get("report_basis_resolution_mode")
        ),
        "report_basis_resolution_reasons": report_basis_resolution_reasons,
        "report_basis_resolution_mode": maybe_text(
            report_basis_resolution.get("report_basis_resolution_mode")
        )
        or maybe_text(report_basis_resolution.get("report_basis_resolution_mode")),
        "report_basis_resolution_reasons": report_basis_resolution_reasons,
        "basis_counts": basis_counts,
        "selected_basis_object_ids": selected_basis_object_ids,
        "supporting_proposal_ids": supporting_proposal_ids,
        "rejected_proposal_ids": rejected_proposal_ids,
        "supporting_opinion_ids": supporting_opinion_ids,
        "rejected_opinion_ids": rejected_opinion_ids,
        "proposal_resolution_records": proposal_resolution_records,
        "proposal_resolution_mode_counts": (
            report_basis_resolution.get("proposal_resolution_mode_counts", {})
            if isinstance(report_basis_resolution.get("proposal_resolution_mode_counts"), dict)
            else {}
        ),
        "council_input_counts": (
            report_basis_resolution.get("council_input_counts", {})
            if isinstance(report_basis_resolution.get("council_input_counts"), dict)
            else {}
        ),
        "frozen_basis": frozen_basis,
        "selected_coverages": selected_coverages,
        "selected_evidence_refs": selected_evidence_refs,
        "evidence_refs": selected_evidence_refs,
        "lineage": lineage,
        "provenance": {
            "source_skill": SKILL_NAME,
            "transition_semantics": "freeze-report-basis",
            "decision_source": decision_source,
            "readiness_source": maybe_text(contract_fields.get("readiness_source")),
            "coverage_source": maybe_text(contract_fields.get("coverage_source")),
            "next_actions_source": maybe_text(
                contract_fields.get("next_actions_source")
            ),
            "report_basis_resolution_mode": maybe_text(
                report_basis_resolution.get("report_basis_resolution_mode")
            ),
            "report_basis_resolution_mode": maybe_text(
                report_basis_resolution.get("report_basis_resolution_mode")
            )
            or maybe_text(report_basis_resolution.get("report_basis_resolution_mode")),
            "transition_request_id": maybe_text(
                transition_request.get("request_id")
            ),
            "council_input_counts": (
                report_basis_resolution.get("council_input_counts", {})
                if isinstance(report_basis_resolution.get("council_input_counts"), dict)
                else {}
            ),
        },
        "board_brief_excerpt": brief_text[:300],
        "gate_reasons": merged_gate_reasons,
        "remaining_risks": remaining_risks,
        "report_basis_notes": (
            "Round is ready and a DB-backed report evidence basis has been frozen for downstream reporting."
            if report_basis_status == "frozen" and not selected_coverages
            else (
                "Round is ready and a DB-backed report evidence basis plus route-gated empirical support has been frozen for downstream reporting."
                if report_basis_status == "frozen"
                else (
                    maybe_text(report_basis_resolution_reasons[0])
                    or "Round is not yet ready; the basis artifact freezes the current DB-backed report-basis posture while report_basis remains withheld."
                )
            )
        ),
    }
    wrapper = store_report_basis_freeze_record(
        run_dir_path,
        report_basis_payload=wrapper,
        artifact_path=str(output_file),
    )
    write_json_file(output_file, wrapper)
    mark_transition_request_committed(
        run_dir_path,
        request_id=maybe_text(transition_request.get("request_id")),
        committed_by_role=maybe_text(
            transition_request.get("required_approval_role")
        )
        or maybe_text(transition_request.get("latest_decision_by_role"))
        or "runtime-operator",
        committed_object_kind="report-basis-freeze",
        committed_object_id=basis_id,
    )

    artifact_refs = [{"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"}]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "basis_id": basis_id,
            "basis_object_kind": "report-basis-freeze",
            "transition_semantics": "freeze-report-basis",
            "report_basis_status": report_basis_status,
            "board_state_source": contract_fields["board_state_source"],
            "coverage_source": contract_fields["coverage_source"],
            "readiness_source": maybe_text(contract_fields.get("readiness_source")),
            "board_brief_source": maybe_text(contract_fields.get("board_brief_source")),
            "next_actions_source": maybe_text(contract_fields.get("next_actions_source")),
            "db_path": contract_fields["db_path"],
            "transition_request_id": maybe_text(transition_request.get("request_id")),
        },
        "receipt_id": "report-basis-freeze-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, basis_id)[:20],
        "batch_id": "reportbasisfreeze-" + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [basis_id],
        "warnings": warnings,
        "deliberation_sync": contract_fields["deliberation_sync"],
        "analysis_sync": contract_fields["analysis_sync"],
        "board_handoff": {
            "candidate_ids": unique_texts([basis_id] + selected_basis_object_ids),
            "evidence_refs": artifact_refs,
            "gap_hints": (
                []
                if report_basis_status == "frozen"
                else merged_gate_reasons[:3]
                or ["Round readiness is not yet sufficient for report-basis freeze."]
            ),
            "challenge_hints": [],
            "suggested_next_skills": [],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Promote round evidence into a compact basis artifact.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--transition-request-id", required=True)
    parser.add_argument("--readiness-path", default="")
    parser.add_argument("--board-brief-path", default="")
    parser.add_argument("--coverage-path", default="")
    parser.add_argument("--next-actions-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--allow-non-ready", action="store_true")
    parser.add_argument("--max-coverages", type=int, default=3)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = freeze_report_basis_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        transition_request_id=args.transition_request_id,
        readiness_path=args.readiness_path,
        board_brief_path=args.board_brief_path,
        coverage_path=args.coverage_path,
        next_actions_path=args.next_actions_path,
        output_path=args.output_path,
        allow_non_ready=args.allow_non_ready,
        max_coverages=args.max_coverages,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
