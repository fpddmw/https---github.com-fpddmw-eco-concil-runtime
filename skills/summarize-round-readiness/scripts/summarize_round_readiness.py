#!/usr/bin/env python3
"""Summarize round-level readiness from board, D1, and evidence artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "summarize-round-readiness"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.objects.council import (  # noqa: E402
    query_council_objects,
)
from eco_council_runtime.kernel.governance.fallback.common import (  # noqa: E402
    maybe_text,
    resolve_path,
)
from eco_council_runtime.kernel.governance.fallback.context import (  # noqa: E402
    load_d1_shared_context,
)
from eco_council_runtime.kernel.governance.fallback.contracts import (  # noqa: E402
    d1_contract_fields_from_payload,
)
from eco_council_runtime.kernel.execution.governed_council_execution import (  # noqa: E402
    COUNCIL_EXECUTION_MODE_FALLBACK_ONLY,
    COUNCIL_EXECUTION_MODE_PROPOSAL_AUTHORITATIVE,
    VALID_COUNCIL_EXECUTION_MODES,
    normalize_council_execution_mode,
)
from eco_council_runtime.challenger_constraints import (  # noqa: E402
    challenger_constraint_state_from_review_comments,
    review_comment_is_open,
)
from eco_council_runtime.kernel.execution.governed_execution_action_semantics import (  # noqa: E402
    action_is_readiness_blocker,
)
from eco_council_runtime.kernel.planes.deliberation_plane import (  # noqa: E402
    store_round_readiness_assessment,
)
from eco_council_runtime.kernel.source_queue.source_queue_contract import (  # noqa: E402
    derive_verification_scope,
)
from eco_council_runtime.reporting_objects import (  # noqa: E402
    query_reporting_objects,
)
from eco_council_runtime.kernel.operator.surfaces import (  # noqa: E402
    load_falsification_probe_wrapper,
    load_next_actions_wrapper,
)


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


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_json_object_if_exists(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def list_items(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


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


def summarize_counts(items: list[dict[str, Any]], *, field_name: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        value = maybe_text(item.get(field_name))
        if not value:
            continue
        counts[value] = counts.get(value, 0) + 1
    return counts


def collected_evidence_refs(items: list[dict[str, Any]]) -> list[str]:
    values: list[Any] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        values.extend(list_items(item.get("evidence_refs")))
    return unique_texts(values)


def load_mission_payload(run_dir: Path) -> dict[str, Any]:
    for candidate in (run_dir / "mission.json", run_dir / "inputs" / "mission.json"):
        payload = read_json_object_if_exists(candidate)
        if payload:
            return payload
    return {}


def selected_source_skills_from_mission(mission: dict[str, Any]) -> list[str]:
    values: list[Any] = []
    selections = (
        mission.get("source_selections")
        if isinstance(mission.get("source_selections"), dict)
        else {}
    )
    for selection in selections.values():
        if isinstance(selection, dict):
            values.extend(list_items(selection.get("selected_sources")))
    return unique_texts(values)


def completed_source_skills_for_round(run_dir: Path, *, round_id: str) -> list[str]:
    execution = read_json_object_if_exists(
        run_dir / "runtime" / f"import_execution_{round_id}.json"
    )
    values: list[Any] = []
    statuses = execution.get("statuses", []) if isinstance(execution.get("statuses"), list) else []
    for status in statuses:
        if not isinstance(status, dict):
            continue
        if maybe_text(status.get("status")) != "completed":
            continue
        values.append(status.get("source_skill"))
    return unique_texts(values)


def explicit_source_skill_equivalents(
    verification_scope: dict[str, Any],
) -> dict[str, set[str]]:
    raw_value = verification_scope.get("source_skill_equivalents")
    if not isinstance(raw_value, dict):
        return {}
    equivalents: dict[str, set[str]] = {}
    for required_source, observed_sources in raw_value.items():
        required_text = maybe_text(required_source)
        if not required_text:
            continue
        equivalents[required_text] = {
            source
            for source in unique_texts(list_items(observed_sources))
            if source
        }
        equivalents[required_text].add(required_text)
    return equivalents


def source_skill_satisfied_by(
    required_source_skill: str,
    observed_source_skills: list[str],
    *,
    explicit_equivalents: dict[str, set[str]],
) -> bool:
    allowed = explicit_equivalents.get(required_source_skill, {required_source_skill})
    observed_lookup = {maybe_text(source).casefold() for source in observed_source_skills}
    return any(source.casefold() in observed_lookup for source in allowed)


def required_lane_ids(verification_scope: dict[str, Any]) -> list[str]:
    lanes = (
        verification_scope.get("required_evidence_lanes")
        if isinstance(verification_scope.get("required_evidence_lanes"), list)
        else []
    )
    return unique_texts(
        [
            lane.get("lane_id")
            for lane in lanes
            if isinstance(lane, dict) and maybe_text(lane.get("lane_id"))
        ]
    )


def required_lane_entries(verification_scope: dict[str, Any]) -> list[dict[str, Any]]:
    lanes = (
        verification_scope.get("required_evidence_lanes")
        if isinstance(verification_scope.get("required_evidence_lanes"), list)
        else []
    )
    return [
        lane
        for lane in lanes
        if isinstance(lane, dict) and maybe_text(lane.get("lane_id"))
    ]


def relation_packet_status(run_dir: Path, *, round_id: str) -> dict[str, Any]:
    packet_path = run_dir / "reporting" / f"spatiotemporal_relation_evidence_packet_{round_id}.json"
    packet = read_json_object_if_exists(packet_path)
    if not packet:
        return {
            "status": "missing",
            "packet_path": str(packet_path),
            "relation_count": 0,
            "basis_object_write_count": 0,
        }
    summary = packet.get("relation_cues_summary") if isinstance(packet.get("relation_cues_summary"), dict) else {}
    basis_handoff = packet.get("basis_handoff") if isinstance(packet.get("basis_handoff"), dict) else {}
    written_records = basis_handoff.get("written_records") if isinstance(basis_handoff.get("written_records"), dict) else {}
    return {
        "status": maybe_text(packet.get("status")) or "unknown",
        "packet_path": str(packet_path),
        "relation_count": int(summary.get("relation_count") or 0),
        "basis_object_write_count": len(written_records),
    }


def report_section_requirement_status(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    requirement: dict[str, Any],
) -> dict[str, Any]:
    payload = query_reporting_objects(
        run_dir,
        object_kind="report-section-draft",
        run_id=run_id,
        round_id=round_id,
        limit=200,
    )
    sections = (
        payload.get("objects", [])
        if isinstance(payload.get("objects"), list)
        else []
    )
    section_keys = {
        maybe_text(key).casefold()
        for key in list_items(requirement.get("section_keys"))
        if maybe_text(key)
    }
    requires_evidence_refs = bool(requirement.get("requires_evidence_refs"))
    candidate_sections: list[dict[str, Any]] = []
    for section in sections:
        if not isinstance(section, dict):
            continue
        section_key = maybe_text(section.get("section_key")).casefold()
        if section_keys and section_key not in section_keys:
            continue
        if maybe_text(section.get("status")).casefold() in {
            "withdrawn",
            "rejected",
            "superseded",
        }:
            continue
        if requires_evidence_refs and not list_items(section.get("evidence_refs")):
            continue
        candidate_sections.append(section)
    return {
        "status": "satisfied" if candidate_sections else "missing",
        "section_count": len(candidate_sections),
        "section_ids": unique_texts(
            [section.get("section_id") for section in candidate_sections]
        ),
        "section_keys": unique_texts(
            [section.get("section_key") for section in candidate_sections]
        ),
    }


def relation_packet_requirement_status(
    run_dir: Path,
    *,
    round_id: str,
    requirement: dict[str, Any],
) -> dict[str, Any]:
    artifact_path = maybe_text(requirement.get("artifact_path"))
    if artifact_path:
        path = Path(artifact_path).expanduser()
        if not path.is_absolute():
            path = run_dir / path
        packet = read_json_object_if_exists(path.resolve())
        status = maybe_text(packet.get("status")) if packet else "missing"
        summary = packet.get("relation_cues_summary") if isinstance(packet.get("relation_cues_summary"), dict) else {}
        observed = {
            "status": status or "unknown",
            "packet_path": str(path.resolve()),
            "relation_count": int(summary.get("relation_count") or 0),
        }
    else:
        observed = relation_packet_status(run_dir, round_id=round_id)
    minimum_relation_count = requirement.get("minimum_relation_count")
    if minimum_relation_count is not None:
        try:
            required_count = int(minimum_relation_count)
        except (TypeError, ValueError):
            required_count = 0
    else:
        required_count = 0
    satisfied = maybe_text(observed.get("status")) == "completed"
    if required_count > 0:
        satisfied = satisfied and int(observed.get("relation_count") or 0) >= required_count
    return {
        **observed,
        "status": "satisfied" if satisfied else "missing",
        "minimum_relation_count": required_count,
    }


def lane_explicitly_scoped_out(lane_id: str, opinions: list[dict[str, Any]]) -> bool:
    marker = f"scope-out:{lane_id}"
    for opinion in opinions:
        if readiness_bucket(opinion) != "ready":
            continue
        referenced_values: list[Any] = []
        for field_name in ("basis_object_ids", "lineage", "evidence_refs"):
            referenced_values.extend(list_items(opinion.get(field_name)))
        if marker in {maybe_text(value) for value in referenced_values}:
            return True
    return False


def required_lane_evidence_review(
    *,
    verification_scope: dict[str, Any],
    run_dir: Path,
    run_id: str,
    round_id: str,
    opinions: list[dict[str, Any]],
) -> dict[str, Any]:
    lane_entries = required_lane_entries(verification_scope)
    lane_ids = unique_texts([lane.get("lane_id") for lane in lane_entries])
    missing_lanes: list[dict[str, Any]] = []
    satisfied_lanes: list[dict[str, Any]] = []
    scoped_out_lanes: list[str] = []
    not_evaluated_lanes: list[dict[str, Any]] = []
    reviewed_lanes: list[str] = []

    for lane in lane_entries:
        lane_id = maybe_text(lane.get("lane_id"))
        requirements = [
            requirement
            for requirement in list_items(lane.get("evidence_requirements"))
            if isinstance(requirement, dict)
        ]
        if lane_explicitly_scoped_out(lane_id, opinions):
            scoped_out_lanes.append(lane_id)
            continue
        if not requirements:
            not_evaluated_lanes.append(
                {
                    "lane_id": lane_id,
                    "status": "not-evaluated",
                    "review_note": (
                        "No explicit evidence_requirements were supplied for this lane, "
                        "so readiness records the lane without inferring expected evidence."
                    ),
                }
            )
            continue
        reviewed_lanes.append(lane_id)
        lane_missing: list[dict[str, Any]] = []
        lane_satisfied: list[dict[str, Any]] = []
        for index, requirement in enumerate(requirements, start=1):
            object_kind = maybe_text(requirement.get("evidence_object_kind"))
            requirement_id = (
                maybe_text(requirement.get("requirement_id"))
                or f"{lane_id}:evidence-requirement:{index}"
            )
            if object_kind == "spatiotemporal-relation-evidence-packet":
                observed = relation_packet_requirement_status(
                    run_dir,
                    round_id=round_id,
                    requirement=requirement,
                )
            elif object_kind == "report-section-draft":
                observed = report_section_requirement_status(
                    run_dir,
                    run_id=run_id,
                    round_id=round_id,
                    requirement=requirement,
                )
            else:
                observed = {
                    "status": "not-evaluated",
                    "review_note": (
                        "Unsupported evidence_object_kind for structural readiness review. "
                        "No expected evidence was inferred."
                    ),
                }
            row = {
                "lane_id": lane_id,
                "requirement_id": requirement_id,
                "evidence_object_kind": object_kind,
                "available_support_skills": list_items(requirement.get("available_support_skills")),
                "observed": observed,
            }
            if maybe_text(observed.get("status")) == "satisfied":
                lane_satisfied.append(row)
            elif maybe_text(observed.get("status")) == "missing":
                lane_missing.append(row)
            else:
                not_evaluated_lanes.append({**row, "status": "not-evaluated"})
        if lane_missing:
            missing_lanes.append(
                {
                    "lane_id": lane_id,
                    "status": "missing-lane-evidence",
                    "missing_requirements": lane_missing,
                    "satisfied_requirements": lane_satisfied,
                    "review_note": (
                        "Explicit lane evidence requirements are not fully materialized. "
                        "This is recorded for council review and does not by itself override readiness."
                    ),
                }
            )
        elif lane_satisfied:
            satisfied_lanes.append(
                {
                    "lane_id": lane_id,
                    "status": "satisfied",
                    "satisfied_requirements": lane_satisfied,
                }
            )

    if missing_lanes:
        status = "missing-lane-evidence"
    elif satisfied_lanes or scoped_out_lanes:
        status = "satisfied"
    elif lane_ids:
        status = "not-evaluated"
    else:
        status = "not-required"
    return {
        "status": status,
        "required_lane_ids": lane_ids,
        "reviewed_lane_ids": unique_texts(reviewed_lanes),
        "missing_lanes": missing_lanes,
        "satisfied_lanes": satisfied_lanes,
        "scoped_out_lanes": scoped_out_lanes,
        "not_evaluated_lanes": not_evaluated_lanes,
    }


def verification_scope_source_gate(
    *,
    mission: dict[str, Any],
    run_dir: Path,
    round_id: str,
) -> dict[str, Any]:
    if not mission:
        return {
            "status": "not-applicable",
            "required_source_skills": [],
            "candidate_source_skills": [],
            "selected_source_skills": [],
            "completed_source_skills": [],
            "missing_required_source_skills": [],
            "missing_candidate_source_skills": [],
            "missing_selected_source_skills": [],
            "gate_reason": "",
            "verification_scope": {},
        }
    verification_scope = (
        mission.get("verification_scope")
        if isinstance(mission.get("verification_scope"), dict)
        else derive_verification_scope(mission)
    )
    required_source_skills = unique_texts(
        list_items(verification_scope.get("required_source_skills"))
    )
    candidate_source_skills = unique_texts(
        list_items(verification_scope.get("candidate_source_skills"))
    )
    selected_source_skills = selected_source_skills_from_mission(mission)
    completed_source_skills = completed_source_skills_for_round(
        run_dir,
        round_id=round_id,
    )
    explicit_equivalents = explicit_source_skill_equivalents(verification_scope)
    missing_required = [
        source
        for source in required_source_skills
        if not source_skill_satisfied_by(
            source,
            completed_source_skills,
            explicit_equivalents=explicit_equivalents,
        )
    ]
    missing_selected = [
        source
        for source in required_source_skills
        if not source_skill_satisfied_by(
            source,
            selected_source_skills,
            explicit_equivalents=explicit_equivalents,
        )
    ]
    missing_candidate = [
        source
        for source in candidate_source_skills
        if not source_skill_satisfied_by(
            source,
            completed_source_skills,
            explicit_equivalents=explicit_equivalents,
        )
    ]
    if not required_source_skills:
        status = "not-required"
    elif missing_required:
        status = "missing-required-source-imports"
    else:
        status = "satisfied"
    reason = ""
    if missing_required:
        reason = (
            "Explicit verification scope requires completed source imports before report-basis freeze: "
            + ", ".join(missing_required)
            + "."
        )
    return {
        "status": status,
        "required_source_skills": required_source_skills,
        "candidate_source_skills": candidate_source_skills,
        "selected_source_skills": selected_source_skills,
        "completed_source_skills": completed_source_skills,
        "missing_required_source_skills": missing_required,
        "missing_candidate_source_skills": missing_candidate,
        "missing_selected_source_skills": missing_selected,
        "gate_reason": reason,
        "verification_scope": verification_scope,
        "source_skill_equivalents": {
            key: sorted(values)
            for key, values in sorted(explicit_equivalents.items())
        },
    }


def load_council_readiness_opinions(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
) -> list[dict[str, Any]]:
    payload = query_council_objects(
        run_dir,
        object_kind="readiness-opinion",
        run_id=run_id,
        round_id=round_id,
        limit=200,
    )
    opinions = (
        payload.get("objects", [])
        if isinstance(payload.get("objects"), list)
        else []
    )
    return [
        opinion
        for opinion in opinions
        if isinstance(opinion, dict)
        and maybe_text(opinion.get("opinion_status")) not in {"withdrawn", "retracted"}
    ]


def load_round_review_comments(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
) -> list[dict[str, Any]]:
    payload = query_council_objects(
        run_dir,
        object_kind="review-comment",
        run_id=run_id,
        round_id=round_id,
        limit=200,
    )
    comments = (
        payload.get("objects", [])
        if isinstance(payload.get("objects"), list)
        else []
    )
    return [comment for comment in comments if isinstance(comment, dict)]


def readiness_bucket(opinion: dict[str, Any]) -> str:
    readiness_value = maybe_text(opinion.get("readiness_status"))
    if bool(opinion.get("sufficient_for_report_basis")) or readiness_value in {
        "ready",
        "ready-for-report-basis",
        "freeze-report-basis",
    }:
        return "ready"
    if readiness_value in {"blocked", "reject", "rejected"}:
        return "blocked"
    return "needs-more-data"


def aggregate_council_readiness_opinions(
    opinions: list[dict[str, Any]],
) -> dict[str, Any]:
    ready_opinions = [
        opinion for opinion in opinions if readiness_bucket(opinion) == "ready"
    ]
    blocked_opinions = [
        opinion for opinion in opinions if readiness_bucket(opinion) == "blocked"
    ]
    needs_more_data_opinions = [
        opinion
        for opinion in opinions
        if readiness_bucket(opinion) == "needs-more-data"
    ]
    if ready_opinions and not blocked_opinions and not needs_more_data_opinions:
        readiness_value = "ready"
        lead_reason = (
            f"Council submitted {len(ready_opinions)} readiness opinions and all support report-basis freeze."
        )
    elif blocked_opinions and not ready_opinions:
        readiness_value = "blocked"
        lead_reason = (
            f"Council submitted {len(opinions)} readiness opinions and none support report-basis freeze."
        )
    else:
        readiness_value = "needs-more-data"
        lead_reason = (
            f"Council readiness opinions are mixed across {len(opinions)} submitted opinions, so the round stays open."
        )
    detailed_reasons = [
        f"{maybe_text(opinion.get('agent_role')) or 'agent'}: {maybe_text(opinion.get('rationale'))}"
        for opinion in opinions
        if maybe_text(opinion.get("rationale"))
    ]
    opinion_ids = unique_texts(
        [opinion.get("opinion_id") for opinion in opinions]
    )
    basis_object_ids = unique_texts(
        [
            basis_object_id
            for opinion in opinions
            for basis_object_id in list_items(opinion.get("basis_object_ids"))
        ]
    )
    evidence_refs = unique_texts(
        [
            evidence_ref
            for opinion in opinions
            for evidence_ref in list_items(opinion.get("evidence_refs"))
        ]
    )
    return {
        "readiness_status": readiness_value,
        "reasons": [lead_reason, *detailed_reasons[:3]],
        "opinion_ids": opinion_ids,
        "basis_object_ids": basis_object_ids,
        "evidence_refs": evidence_refs,
        "opinion_status_counts": {
            "ready": len(ready_opinions),
            "blocked": len(blocked_opinions),
            "needs-more-data": len(needs_more_data_opinions),
        },
    }


def readiness_status(
    *,
    active_hypotheses: int,
    issue_cluster_count: int,
    empirical_issue_count: int,
    observation_lane_issue_count: int,
    observation_lane_gap_count: int,
    formal_record_issue_count: int,
    public_discourse_issue_count: int,
    stakeholder_deliberation_issue_count: int,
    strong_coverages: int,
    moderate_coverages: int,
    open_challenges: int,
    open_tasks: int,
    open_probes: int,
    high_priority_actions: int,
    routing_actions: int,
    empirical_gap_actions: int,
    representation_gap_actions: int,
    formal_linkage_actions: int,
    issue_gap_actions: int,
    relation_gap_actions: int,
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if active_hypotheses == 0 and issue_cluster_count == 0:
        reasons.append("No active board hypotheses or controversy-map issues are available for round-level review.")
        return "blocked", reasons
    if open_challenges > 0:
        reasons.append(f"{open_challenges} contested points remain open.")
    if open_tasks > 0:
        reasons.append(f"{open_tasks} board coordination tasks remain in flight.")
    if open_probes > 0:
        reasons.append(f"{open_probes} controversy probes remain open.")
    if routing_actions > 0:
        reasons.append(f"{routing_actions} verification-routing actions remain unresolved.")
    if observation_lane_issue_count > 0 and max(empirical_gap_actions, observation_lane_gap_count) > 0:
        reasons.append(
            f"{max(empirical_gap_actions, observation_lane_gap_count)} route-gated empirical verification or contradiction-resolution actions remain unresolved."
        )
    if representation_gap_actions > 0:
        reasons.append(f"{representation_gap_actions} representation-gap actions remain unresolved.")
    if formal_linkage_actions > 0 and representation_gap_actions == 0:
        reasons.append(f"{formal_linkage_actions} formal/public linkage actions remain unresolved.")
    if issue_gap_actions > 0:
        reasons.append(f"{issue_gap_actions} issue-structure or contestation actions remain unresolved.")
    if relation_gap_actions > 0:
        reasons.append(f"{relation_gap_actions} spatiotemporal relation gap actions remain unresolved.")
    if high_priority_actions > 0 and not reasons:
        reasons.append(f"{high_priority_actions} high-priority investigation actions remain unresolved.")
    if reasons:
        return "needs-more-data", reasons
    if observation_lane_issue_count > 0 and strong_coverages > 0:
        reasons.append("Explicit observation-lane issues are backed by at least one strong empirical support object and no structural blockers remain.")
    elif observation_lane_issue_count > 0 and moderate_coverages > 0:
        reasons.append("Explicit observation-lane issues are covered at least moderately and no remaining structural blockers are visible.")
    elif observation_lane_issue_count > 0:
        reasons.append("Explicit observation-lane issues no longer carry unresolved verification blockers and the current controversy basis is coherent enough for report-basis review.")
    elif empirical_issue_count == 0 and (
        formal_record_issue_count > 0
        or public_discourse_issue_count > 0
        or stakeholder_deliberation_issue_count > 0
    ):
        reasons.append("No route-gated empirical blockers remain and the current formal/public/discourse issue structure is coherent enough for report-basis review.")
    else:
        reasons.append("No route-gated empirical blockers remain and the current issue / route / linkage structure is coherent enough for report-basis review.")
    return "ready", reasons


def summarize_round_readiness_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    board_summary_path: str,
    board_brief_path: str,
    next_actions_path: str,
    probes_path: str,
    coverage_path: str,
    output_path: str,
    council_execution_mode: str,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    board_summary_file = resolve_path(run_dir_path, board_summary_path, f"board/board_state_summary_{round_id}.json")
    board_brief_file = resolve_path(run_dir_path, board_brief_path, f"board/board_brief_{round_id}.md")
    next_actions_file = resolve_path(run_dir_path, next_actions_path, f"investigation/next_actions_{round_id}.json")
    probes_file = resolve_path(run_dir_path, probes_path, f"investigation/falsification_probes_{round_id}.json")
    output_file = resolve_path(run_dir_path, output_path, f"reporting/round_readiness_{round_id}.json")

    shared_context = load_d1_shared_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        board_summary_path=board_summary_path,
        board_brief_path=board_brief_path,
        coverage_path=coverage_path,
        include_board_notes=True,
    )
    warnings = (
        shared_context.get("warnings", [])
        if isinstance(shared_context.get("warnings"), list)
        else []
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
    next_actions_artifact_present = bool(next_actions_context.get("artifact_present"))
    next_actions_present = bool(next_actions_context.get("payload_present"))
    next_actions = next_actions_payload if isinstance(next_actions_payload, dict) else {"ranked_actions": [], "action_count": 0}
    probes_context = load_falsification_probe_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        probes_path=probes_path,
    )
    probes_payload = (
        probes_context.get("payload")
        if isinstance(probes_context.get("payload"), dict)
        else None
    )
    probes_artifact_present = bool(probes_context.get("artifact_present"))
    probes_present = bool(probes_context.get("payload_present"))
    probes = probes_payload if isinstance(probes_payload, dict) else {"probes": [], "probe_count": 0}
    council_opinions = load_council_readiness_opinions(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    review_comments = load_round_review_comments(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    mission_payload = load_mission_payload(run_dir_path)
    verification_source_gate = verification_scope_source_gate(
        mission=mission_payload,
        run_dir=run_dir_path,
        round_id=round_id,
    )
    normalized_council_execution_mode = normalize_council_execution_mode(
        council_execution_mode
    )
    contract_fields = d1_contract_fields_from_payload(
        shared_context,
        observed_inputs_overrides={
            "next_actions_artifact_present": next_actions_artifact_present,
            "next_actions_present": next_actions_present,
            "probes_artifact_present": probes_artifact_present,
            "probes_present": probes_present,
        },
    )
    coverages = (
        shared_context.get("coverages", [])
        if isinstance(shared_context.get("coverages"), list)
        else []
    )
    coverage_file = maybe_text(shared_context.get("coverage_file"))
    brief_excerpt = maybe_text(shared_context.get("board_brief_text"))[:220]
    board_state = (
        shared_context.get("board_state")
        if isinstance(shared_context.get("board_state"), dict)
        else {}
    )
    agenda_counts = (
        shared_context.get("agenda_counts")
        if isinstance(shared_context.get("agenda_counts"), dict)
        else {}
    )

    strong_coverages = len([item for item in coverages if maybe_text(item.get("readiness")) == "strong"])
    moderate_coverages = len([item for item in coverages if maybe_text(item.get("readiness")) == "moderate"])
    weak_coverages = len([item for item in coverages if maybe_text(item.get("readiness")) == "weak"])

    counts = board_state.get("counts", {}) if isinstance(board_state.get("counts"), dict) else {}
    active_hypotheses = int(counts.get("hypotheses_active") or len(board_state.get("active_hypotheses", [])))
    open_challenges = int(counts.get("challenge_open") or len(board_state.get("open_challenges", [])))
    open_tasks = int(counts.get("tasks_open") or len(board_state.get("open_tasks", [])))
    issue_cluster_count = int(agenda_counts.get("issue_cluster_count") or 0)
    empirical_issue_count = int(agenda_counts.get("empirical_issue_count") or 0)
    non_empirical_issue_count = int(agenda_counts.get("non_empirical_issue_count") or 0)
    mixed_issue_count = int(agenda_counts.get("mixed_issue_count") or 0)
    observation_lane_issue_count = int(
        agenda_counts.get("observation_lane_issue_count") or 0
    )
    observation_lane_gap_count = int(
        agenda_counts.get("observation_lane_gap_count") or 0
    )
    formal_record_issue_count = int(
        agenda_counts.get("formal_record_issue_count") or 0
    )
    public_discourse_issue_count = int(
        agenda_counts.get("public_discourse_issue_count") or 0
    )
    stakeholder_deliberation_issue_count = int(
        agenda_counts.get("stakeholder_deliberation_issue_count") or 0
    )
    open_probes = len([item for item in probes.get("probes", []) if isinstance(item, dict) and maybe_text(item.get("probe_status")) not in {"closed", "cancelled"}]) if isinstance(probes.get("probes"), list) else 0
    blocking_actions = [
        item
        for item in next_actions.get("ranked_actions", [])
        if isinstance(item, dict) and action_is_readiness_blocker(item)
    ] if isinstance(next_actions.get("ranked_actions"), list) else []
    high_priority_actions = len(
        [
            item
            for item in blocking_actions
            if maybe_text(item.get("priority")) in {"high", "critical"}
        ]
    )
    action_gap_counts = summarize_counts(
        blocking_actions,
        field_name="controversy_gap",
    )
    probe_type_counts = summarize_counts(
        probes.get("probes", []) if isinstance(probes.get("probes"), list) else [],
        field_name="probe_type",
    )
    routing_actions = max(
        int(action_gap_counts.get("verification-routing-gap", 0)),
        int(agenda_counts.get("routing_issue_count") or 0),
    )
    empirical_gap_actions = max(
        int(action_gap_counts.get("verification-gap", 0))
        + int(action_gap_counts.get("formal-public-misalignment", 0)),
        int(agenda_counts.get("empirical_issue_gap_count") or 0),
    )
    representation_gap_actions = max(
        int(action_gap_counts.get("representation-gap", 0)),
        int(agenda_counts.get("representation_gap_count") or 0),
    )
    formal_linkage_actions = max(
        int(action_gap_counts.get("formal-record-gap", 0))
        + int(action_gap_counts.get("formal-public-linkage-gap", 0))
        + int(action_gap_counts.get("public-discourse-gap", 0))
        + int(action_gap_counts.get("stakeholder-deliberation-gap", 0)),
        int(agenda_counts.get("formal_public_linkage_gap_count") or 0),
    )
    issue_gap_actions = int(action_gap_counts.get("issue-structure-gap", 0)) + int(
        action_gap_counts.get("unresolved-contestation", 0)
    )
    relation_gap_actions = len(
        [
            item
            for item in blocking_actions
            if maybe_text(item.get("relation_id"))
            or maybe_text(item.get("action_kind"))
            in {
                "review-spatiotemporal-relation",
                "review-spatiotemporal-relation-alternatives",
            }
            or maybe_text(item.get("controversy_gap"))
            in {
                "spatiotemporal-relation-gap",
                "relation-overclaim-risk",
            }
            or (
                isinstance(item.get("target"), dict)
                and maybe_text(item["target"].get("object_kind"))
                == "spatiotemporal-relation-cue"
            )
        ]
    )
    diffusion_focus_count = int(agenda_counts.get("diffusion_focus_count") or 0)

    status_value, reasons = readiness_status(
        active_hypotheses=active_hypotheses,
        issue_cluster_count=issue_cluster_count,
        empirical_issue_count=empirical_issue_count,
        observation_lane_issue_count=observation_lane_issue_count,
        observation_lane_gap_count=observation_lane_gap_count,
        formal_record_issue_count=formal_record_issue_count,
        public_discourse_issue_count=public_discourse_issue_count,
        stakeholder_deliberation_issue_count=stakeholder_deliberation_issue_count,
        strong_coverages=strong_coverages,
        moderate_coverages=moderate_coverages,
        open_challenges=open_challenges,
        open_tasks=open_tasks,
        open_probes=open_probes,
        high_priority_actions=high_priority_actions,
        routing_actions=routing_actions,
        empirical_gap_actions=empirical_gap_actions,
        representation_gap_actions=representation_gap_actions,
        formal_linkage_actions=formal_linkage_actions,
        issue_gap_actions=issue_gap_actions,
        relation_gap_actions=relation_gap_actions,
    )
    decision_source = "policy-fallback"
    readiness_lineage = unique_texts(
        [
            item.get("action_id")
            for item in next_actions.get("ranked_actions", [])
            if isinstance(item, dict)
        ]
        + [
            item.get("probe_id")
            for item in probes.get("probes", [])
            if isinstance(item, dict)
        ]
    )
    readiness_evidence_refs = unique_texts(
        collected_evidence_refs(coverages)
        + collected_evidence_refs(
            next_actions.get("ranked_actions", [])
            if isinstance(next_actions.get("ranked_actions"), list)
            else []
        )
        + collected_evidence_refs(
            probes.get("probes", [])
            if isinstance(probes.get("probes"), list)
            else []
        )
    )
    selected_basis_object_ids: list[str] = []
    opinion_ids: list[str] = []
    opinion_status_counts: dict[str, int] = {}
    if (
        council_opinions
        and normalized_council_execution_mode
        != COUNCIL_EXECUTION_MODE_FALLBACK_ONLY
    ):
        council_summary = aggregate_council_readiness_opinions(council_opinions)
        status_value = maybe_text(council_summary.get("readiness_status")) or status_value
        reasons = (
            council_summary.get("reasons", [])
            if isinstance(council_summary.get("reasons"), list)
            else reasons
        )
        decision_source = "agent-council"
        selected_basis_object_ids = list_items(
            council_summary.get("basis_object_ids")
        )
        opinion_ids = list_items(council_summary.get("opinion_ids"))
        opinion_status_counts = (
            council_summary.get("opinion_status_counts", {})
            if isinstance(council_summary.get("opinion_status_counts"), dict)
            else {}
        )
        readiness_evidence_refs = unique_texts(
            list_items(council_summary.get("evidence_refs"))
            + readiness_evidence_refs
        )
        readiness_lineage = unique_texts(opinion_ids + selected_basis_object_ids)
    elif council_opinions:
        warnings.append(
            {
                "code": "council-opinions-ignored",
                "message": "Council readiness opinions were present but ignored because council_execution_mode=fallback-only.",
            }
        )
    active_scope_opinions = (
        council_opinions
        if normalized_council_execution_mode != COUNCIL_EXECUTION_MODE_FALLBACK_ONLY
        else []
    )
    lane_evidence_review = required_lane_evidence_review(
        verification_scope=verification_source_gate.get("verification_scope", {})
        if isinstance(verification_source_gate.get("verification_scope"), dict)
        else {},
        run_dir=run_dir_path,
        run_id=run_id,
        round_id=round_id,
        opinions=active_scope_opinions,
    )
    open_review_comments = [
        comment for comment in review_comments if review_comment_is_open(comment)
    ]
    challenger_constraint_state = challenger_constraint_state_from_review_comments(
        review_comments
    )
    unresolved_challenger_constraints = [
        constraint
        for constraint in list_items(
            challenger_constraint_state.get("unresolved_challenger_constraints")
        )
        if isinstance(constraint, dict)
    ]
    blocking_review_comment_ids = unique_texts(
        challenger_constraint_state.get(
            "unresolved_challenger_constraint_review_comment_ids", []
        )
        if isinstance(
            challenger_constraint_state.get(
                "unresolved_challenger_constraint_review_comment_ids"
            ),
            list,
        )
        else []
    )
    if unresolved_challenger_constraints:
        review_reason = (
            f"{len(unresolved_challenger_constraints)} unresolved challenger constraints "
            "require explicit constraint disposition before report-basis freeze."
        )
        if status_value == "ready":
            status_value = "needs-more-data"
        if review_reason not in reasons:
            reasons = [review_reason, *reasons]
        readiness_lineage = unique_texts(readiness_lineage + blocking_review_comment_ids)
        readiness_evidence_refs = unique_texts(
            readiness_evidence_refs
            + collected_evidence_refs(unresolved_challenger_constraints)
        )
    missing_required_source_skills = list_items(
        verification_source_gate.get("missing_required_source_skills")
    )
    missing_candidate_source_skills = list_items(
        verification_source_gate.get("missing_candidate_source_skills")
    )
    if missing_required_source_skills:
        gate_reason = maybe_text(verification_source_gate.get("gate_reason"))
        if status_value == "ready":
            status_value = "needs-more-data"
        if gate_reason and gate_reason not in reasons:
            reasons = [gate_reason, *reasons]
    missing_required_lane_evidence = [
        lane for lane in list_items(lane_evidence_review.get("missing_lanes")) if isinstance(lane, dict)
    ]
    if missing_required_lane_evidence:
        warnings.append(
            {
                "code": "required-lane-evidence-not-materialized",
                "message": (
                    f"{len(missing_required_lane_evidence)} required evidence lanes have no "
                    "materialized lane evidence object. This is recorded for council review "
                    "and does not by itself override readiness opinions."
                ),
            }
        )
    findings = [
        {
            "finding_id": "readiness-route-gating",
            "title": "Route gating posture",
            "summary": (
                f"routing_actions={routing_actions}, observation_lane_issues={observation_lane_issue_count}, "
                f"observation_lane_gap_actions={max(empirical_gap_actions, observation_lane_gap_count)}, "
                f"open_probes={open_probes}, high_priority_actions={high_priority_actions}"
            ),
            "confidence": "medium",
        },
        {
            "finding_id": "readiness-empirical-support",
            "title": "Empirical support posture",
            "summary": f"strong={strong_coverages}, moderate={moderate_coverages}, weak={weak_coverages}",
            "confidence": "medium",
        },
        {
            "finding_id": "readiness-board",
            "title": "Board posture",
            "summary": f"active_hypotheses={active_hypotheses}, open_challenges={open_challenges}, open_tasks={open_tasks}",
            "confidence": "medium",
        },
        {
            "finding_id": "readiness-investigation",
            "title": "Investigation posture",
            "summary": (
                f"formal_record_issues={formal_record_issue_count}, "
                f"public_discourse_issues={public_discourse_issue_count}, "
                f"stakeholder_deliberation_issues={stakeholder_deliberation_issue_count}, "
                f"representation_gap_actions={representation_gap_actions}, "
                f"formal_linkage_actions={formal_linkage_actions}, "
                f"issue_gap_actions={issue_gap_actions}, "
                f"relation_gap_actions={relation_gap_actions}, "
                f"unresolved_challenger_constraints={len(unresolved_challenger_constraints)}"
            ),
            "confidence": "medium",
        },
        {
            "finding_id": "readiness-controversy-map",
            "title": "Controversy map posture",
            "summary": f"issue_clusters={issue_cluster_count}, empirical_issues={empirical_issue_count}, non_empirical_issues={non_empirical_issue_count}, mixed_issues={mixed_issue_count}, formal_linkage_actions={formal_linkage_actions}, diffusion_focus_count={diffusion_focus_count}, action_gaps={json.dumps(action_gap_counts, ensure_ascii=True, sort_keys=True)}",
            "confidence": "medium",
        },
    ]
    if brief_excerpt:
        findings.append({"finding_id": "readiness-brief", "title": "Board brief context", "summary": brief_excerpt, "confidence": "low"})

    if status_value == "ready":
        recommended_next_skills = ["freeze-report-basis"]
    elif (
        council_opinions
        and normalized_council_execution_mode
        != COUNCIL_EXECUTION_MODE_FALLBACK_ONLY
    ):
        recommended_next_skills = [
            "submit-council-proposal",
            "submit-readiness-opinion",
        ]
        if (
            open_probes > 0
            or routing_actions > 0
            or empirical_gap_actions > 0
            or representation_gap_actions > 0
            or formal_linkage_actions > 0
            or issue_gap_actions > 0
            or relation_gap_actions > 0
        ):
            recommended_next_skills.append("open-falsification-probe")
        deduped: list[str] = []
        for skill_name in recommended_next_skills:
            if skill_name not in deduped:
                deduped.append(skill_name)
        recommended_next_skills = deduped
    else:
        recommended_next_skills = [
            "propose-next-actions",
            "submit-council-proposal",
            "submit-readiness-opinion",
        ]
        if open_probes > 0 or routing_actions > 0 or empirical_gap_actions > 0 or relation_gap_actions > 0:
            recommended_next_skills.append("open-falsification-probe")
        deduped: list[str] = []
        for skill_name in recommended_next_skills:
            if skill_name not in deduped:
                deduped.append(skill_name)
        recommended_next_skills = deduped
    if unresolved_challenger_constraints:
        followup_skills = [
            "open-followup-from-review-comment",
            "open-challenge-ticket",
            "claim-board-task",
            "submit-readiness-opinion",
        ]
        recommended_next_skills = unique_texts(
            followup_skills + recommended_next_skills
        )
    if missing_required_source_skills:
        recommended_next_skills = unique_texts(
            [
                "prepare-round",
                "normalize-fetch-execution",
                *missing_required_source_skills,
            ]
            + recommended_next_skills
        )
    wrapper = {
        "schema_version": "d2.1",
        "skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "council_execution_mode": normalized_council_execution_mode,
        "board_summary_path": str(board_summary_file),
        "board_brief_path": str(board_brief_file),
        "next_actions_path": str(next_actions_file),
        "probes_path": str(probes_file),
        "coverage_path": str(coverage_file),
        **contract_fields,
        "next_actions_source": maybe_text(next_actions_context.get("source"))
        or "missing-next-actions",
        "probes_source": maybe_text(probes_context.get("source"))
        or "missing-probes",
        "decision_source": decision_source,
        "readiness_status": status_value,
        "sufficient_for_report_basis": status_value == "ready",
        "evidence_refs": readiness_evidence_refs,
        "lineage": readiness_lineage,
        "provenance": {
            "source_skill": SKILL_NAME,
            "decision_source": decision_source,
            "board_state_source": contract_fields["board_state_source"],
            "coverage_source": contract_fields["coverage_source"],
            "next_actions_source": maybe_text(next_actions_context.get("source"))
            or "missing-next-actions",
            "probes_source": maybe_text(probes_context.get("source"))
            or "missing-probes",
            "council_opinion_count": len(council_opinions),
        },
        "opinion_ids": opinion_ids,
        "selected_basis_object_ids": selected_basis_object_ids,
        "readiness_opinion_count": len(council_opinions),
        "readiness_opinion_status_counts": opinion_status_counts,
        "review_comment_count": len(review_comments),
        "open_review_comment_count": len(open_review_comments),
        "blocking_review_comment_count": len(unresolved_challenger_constraints),
        "blocking_review_comment_ids": blocking_review_comment_ids,
        "challenger_constraint_count": int(
            challenger_constraint_state.get("challenger_constraint_count") or 0
        ),
        "unresolved_challenger_constraint_count": int(
            challenger_constraint_state.get(
                "unresolved_challenger_constraint_count"
            )
            or 0
        ),
        "challenger_constraint_ids": list_items(
            challenger_constraint_state.get("challenger_constraint_ids")
        ),
        "unresolved_challenger_constraint_ids": list_items(
            challenger_constraint_state.get("unresolved_challenger_constraint_ids")
        ),
        "challenger_constraints": list_items(
            challenger_constraint_state.get("challenger_constraints")
        ),
        "unresolved_challenger_constraints": list_items(
            challenger_constraint_state.get("unresolved_challenger_constraints")
        ),
        "basis_use_constraints": list_items(
            challenger_constraint_state.get("basis_use_constraints")
        ),
        "verification_scope": verification_source_gate.get("verification_scope", {}),
        "verification_scope_gate": {
            "status": maybe_text(verification_source_gate.get("status")),
            "required_source_skills": list_items(
                verification_source_gate.get("required_source_skills")
            ),
            "candidate_source_skills": list_items(
                verification_source_gate.get("candidate_source_skills")
            ),
            "selected_source_skills": list_items(
                verification_source_gate.get("selected_source_skills")
            ),
            "completed_source_skills": list_items(
                verification_source_gate.get("completed_source_skills")
            ),
            "missing_required_source_skills": missing_required_source_skills,
            "missing_candidate_source_skills": missing_candidate_source_skills,
            "missing_selected_source_skills": list_items(
                verification_source_gate.get("missing_selected_source_skills")
            ),
            "gate_reason": maybe_text(verification_source_gate.get("gate_reason")),
            "source_skill_equivalents": (
                verification_source_gate.get("source_skill_equivalents", {})
                if isinstance(verification_source_gate.get("source_skill_equivalents"), dict)
                else {}
            ),
        },
        "required_lane_evidence_review": {
            "status": maybe_text(lane_evidence_review.get("status")),
            "required_lane_ids": list_items(lane_evidence_review.get("required_lane_ids")),
            "reviewed_lane_ids": list_items(lane_evidence_review.get("reviewed_lane_ids")),
            "missing_lanes": missing_required_lane_evidence,
            "satisfied_lanes": list_items(lane_evidence_review.get("satisfied_lanes")),
            "scoped_out_lanes": list_items(lane_evidence_review.get("scoped_out_lanes")),
            "not_evaluated_lanes": list_items(lane_evidence_review.get("not_evaluated_lanes")),
        },
        "agenda_counts": agenda_counts,
        "counts": {
            "active_hypotheses": active_hypotheses,
            "issue_clusters": issue_cluster_count,
            "empirical_issues": empirical_issue_count,
            "non_empirical_issues": non_empirical_issue_count,
            "mixed_issues": mixed_issue_count,
            "observation_lane_issues": observation_lane_issue_count,
            "observation_lane_gap_actions": max(
                empirical_gap_actions,
                observation_lane_gap_count,
            ),
            "formal_record_issues": formal_record_issue_count,
            "public_discourse_issues": public_discourse_issue_count,
            "stakeholder_deliberation_issues": stakeholder_deliberation_issue_count,
            "open_challenges": open_challenges,
            "open_tasks": open_tasks,
            "open_probes": open_probes,
            "strong_coverages": strong_coverages,
            "moderate_coverages": moderate_coverages,
            "weak_coverages": weak_coverages,
            "high_priority_actions": high_priority_actions,
            "routing_actions": routing_actions,
            "empirical_gap_actions": empirical_gap_actions,
            "representation_gap_actions": representation_gap_actions,
            "formal_linkage_actions": formal_linkage_actions,
            "issue_gap_actions": issue_gap_actions,
            "relation_gap_actions": relation_gap_actions,
            "diffusion_focus_count": diffusion_focus_count,
            "agent_readiness_opinions": len(council_opinions),
            "open_review_comments": len(open_review_comments),
            "blocking_review_comments": len(unresolved_challenger_constraints),
            "challenger_constraints": int(
                challenger_constraint_state.get("challenger_constraint_count") or 0
            ),
            "unresolved_challenger_constraints": len(
                unresolved_challenger_constraints
            ),
            "required_source_skills": len(
                list_items(verification_source_gate.get("required_source_skills"))
            ),
            "candidate_source_skills": len(
                list_items(verification_source_gate.get("candidate_source_skills"))
            ),
            "missing_required_source_skills": len(missing_required_source_skills),
            "missing_candidate_source_skills": len(missing_candidate_source_skills),
            "missing_required_lane_evidence": len(missing_required_lane_evidence),
        },
        "controversy_gap_counts": action_gap_counts,
        "probe_type_counts": probe_type_counts,
        "gate_reasons": reasons,
        "findings": findings[:5],
        "recommended_next_skills": recommended_next_skills,
    }
    wrapper = store_round_readiness_assessment(
        run_dir_path,
        readiness_payload=wrapper,
        artifact_path=str(output_file),
    )
    write_json_file(output_file, wrapper)
    readiness_id = maybe_text(wrapper.get("readiness_id")) or (
        "round-readiness-" + stable_hash(run_id, round_id, status_value)[:12]
    )
    artifact_refs = [{"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"}]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "readiness_status": status_value,
            "readiness_id": readiness_id,
            "board_state_source": contract_fields["board_state_source"],
            "coverage_source": contract_fields["coverage_source"],
            "db_path": contract_fields["db_path"],
        },
        "receipt_id": "reporting-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, readiness_id)[:20],
        "batch_id": "reportingbatch-" + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [readiness_id],
        "warnings": warnings,
        "deliberation_sync": contract_fields["deliberation_sync"],
        "analysis_sync": contract_fields["analysis_sync"],
        "board_handoff": {
            "candidate_ids": [readiness_id],
            "evidence_refs": artifact_refs,
            "gap_hints": [] if status_value == "ready" else reasons[:3],
            "challenge_hints": [reason for reason in reasons if "challenge" in reason.lower() or "probe" in reason.lower()],
            "suggested_next_skills": recommended_next_skills,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize round-level readiness from board, D1, and evidence artifacts.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--board-summary-path", default="")
    parser.add_argument("--board-brief-path", default="")
    parser.add_argument("--next-actions-path", default="")
    parser.add_argument("--probes-path", default="")
    parser.add_argument("--coverage-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument(
        "--council-execution-mode",
        choices=sorted(VALID_COUNCIL_EXECUTION_MODES),
        default=COUNCIL_EXECUTION_MODE_PROPOSAL_AUTHORITATIVE,
    )
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = summarize_round_readiness_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        board_summary_path=args.board_summary_path,
        board_brief_path=args.board_brief_path,
        next_actions_path=args.next_actions_path,
        probes_path=args.probes_path,
        coverage_path=args.coverage_path,
        output_path=args.output_path,
        council_execution_mode=args.council_execution_mode,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
