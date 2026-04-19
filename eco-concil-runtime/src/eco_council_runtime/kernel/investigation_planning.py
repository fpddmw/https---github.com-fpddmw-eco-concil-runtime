from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from .analysis_plane import (
    load_claim_verifiability_context,
    load_controversy_map_context,
    load_diffusion_edge_context,
    load_evidence_coverage_context,
    load_formal_public_link_context,
    load_representation_gap_context,
    load_verification_route_context,
)
from .deliberation_plane import load_round_snapshot
from .phase2_state_surfaces import (
    load_council_decision_wrapper,
    load_expert_report_wrapper,
    load_falsification_probe_wrapper,
    load_final_publication_wrapper,
    load_next_actions_wrapper,
    load_promotion_basis_wrapper,
    load_reporting_handoff_wrapper,
    load_round_readiness_wrapper,
)
# Compatibility-only re-exports. Canonical phase-2 DB/artifact state surfaces
# now live in phase2_state_surfaces.py and internal callers should import there.

PRIORITY_WEIGHT = {"critical": 4.0, "high": 3.0, "medium": 2.0, "low": 1.0}
OBSERVED_INPUT_PREFIXES = (
    "board_summary",
    "board_brief",
    "coverage",
    "next_actions",
    "probes",
    "controversy_map",
    "verification_route",
    "claim_verifiability",
    "formal_public_links",
    "representation_gap",
    "diffusion_edges",
)
ARTIFACT_FALLBACK_PREFIXES = {
    "board_summary",
    "board_brief",
    "next_actions",
    "probes",
}
EXPLICIT_D1_INPUT_KEYS = {
    key
    for prefix in OBSERVED_INPUT_PREFIXES
    for key in (f"{prefix}_artifact_present", f"{prefix}_present")
}


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


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


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


def excerpt_text(text: str, limit: int = 180) -> str:
    normalized = maybe_text(text)
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3].rstrip() + "..."


def priority_score(priority: str) -> float:
    return PRIORITY_WEIGHT.get(maybe_text(priority).lower(), PRIORITY_WEIGHT["medium"])


def list_field(item: dict[str, Any], key: str) -> list[str]:
    values = item.get(key)
    if not isinstance(values, list):
        return []
    return [maybe_text(value) for value in values if maybe_text(value)]


def source_available(value: Any) -> bool:
    text = maybe_text(value)
    return bool(text) and not text.startswith("missing-")


def optional_context_source(context: dict[str, Any]) -> str:
    for key, value in context.items():
        if key.endswith("_source"):
            return maybe_text(value)
    return ""


def optional_context_warnings(context: dict[str, Any]) -> list[dict[str, Any]]:
    warnings = context.get("warnings", []) if isinstance(context.get("warnings"), list) else []
    if source_available(optional_context_source(context)):
        return warnings
    return []


def optional_context_count(context: dict[str, Any], count_key: str) -> int:
    return int(context.get(count_key) or 0)


def optional_context_present(context: dict[str, Any], count_key: str) -> bool:
    return source_available(optional_context_source(context)) or optional_context_count(context, count_key) > 0


def priority_from_score(score: float) -> str:
    if score >= 0.88:
        return "critical"
    if score >= 0.72:
        return "high"
    if score >= 0.54:
        return "medium"
    return "low"


def role_from_lane(lane: str, *, default_role: str = "moderator") -> str:
    lane_text = maybe_text(lane)
    if lane_text == "environmental-observation":
        return "environmentalist"
    if lane_text in {
        "public-discourse-analysis",
        "stakeholder-deliberation-analysis",
    }:
        return "sociologist"
    if lane_text == "formal-comment-and-policy-record":
        return "moderator"
    return default_role


def issue_label_for_item(item: dict[str, Any]) -> str:
    return (
        maybe_text(item.get("issue_label"))
        or maybe_text(item.get("issue_hint"))
        or maybe_text(item.get("claim_id"))
        or "public controversy"
    )


def grouped_by_issue_label(items: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        issue_label = issue_label_for_item(item)
        grouped.setdefault(issue_label, []).append(item)
    return grouped


def indexed_by_claim_id(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        claim_id = maybe_text(item.get("claim_id"))
        if not claim_id:
            continue
        indexed[claim_id] = item
    return indexed


def weakest_coverage_for_claim_ids(
    claim_ids: list[str],
    coverages_by_claim_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for claim_id in claim_ids:
        claim_text = maybe_text(claim_id)
        if not claim_text:
            continue
        candidate = coverages_by_claim_id.get(claim_text)
        if isinstance(candidate, dict) and candidate:
            candidates.append(candidate)
    if not candidates:
        return {}
    return sorted(
        candidates,
        key=lambda item: (
            -int(item.get("contradiction_link_count") or 0),
            float(item.get("coverage_score") or 0.0),
            maybe_text(item.get("coverage_id")),
        ),
    )[0]


def normalize_d1_observed_inputs(
    observed_inputs: dict[str, Any] | None = None,
    **overrides: Any,
) -> dict[str, Any]:
    source: dict[str, Any] = {}
    if isinstance(observed_inputs, dict):
        source.update(observed_inputs)
    source.update(overrides)
    normalized = {
        key: value for key, value in source.items() if key not in EXPLICIT_D1_INPUT_KEYS
    }
    for prefix in OBSERVED_INPUT_PREFIXES:
        present_key = f"{prefix}_present"
        artifact_key = f"{prefix}_artifact_present"
        present_value = bool(source.get(present_key, False))
        if prefix in ARTIFACT_FALLBACK_PREFIXES:
            artifact_value = bool(source.get(artifact_key, present_value))
        else:
            artifact_value = bool(source.get(artifact_key, False))
        normalized[artifact_key] = artifact_value
        normalized[present_key] = present_value
    return normalized


def d1_contract_fields(
    *,
    board_state_source: Any = "",
    coverage_source: Any = "",
    db_path: Any = "",
    deliberation_sync: dict[str, Any] | None = None,
    analysis_sync: dict[str, Any] | None = None,
    observed_inputs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "board_state_source": maybe_text(board_state_source) or "missing-board",
        "coverage_source": maybe_text(coverage_source) or "missing-coverage",
        "db_path": maybe_text(db_path),
        "deliberation_sync": deliberation_sync if isinstance(deliberation_sync, dict) else {},
        "analysis_sync": analysis_sync if isinstance(analysis_sync, dict) else {},
        "observed_inputs": normalize_d1_observed_inputs(observed_inputs),
    }


def d1_contract_fields_from_payload(
    payload: dict[str, Any] | None,
    *,
    observed_inputs_overrides: dict[str, Any] | None = None,
) -> dict[str, Any]:
    source = payload if isinstance(payload, dict) else {}
    observed_inputs = (
        source.get("observed_inputs")
        if isinstance(source.get("observed_inputs"), dict)
        else {}
    )
    if isinstance(observed_inputs_overrides, dict):
        observed_inputs = {**observed_inputs, **observed_inputs_overrides}
    return d1_contract_fields(
        board_state_source=source.get("board_state_source"),
        coverage_source=source.get("coverage_source"),
        db_path=source.get("db_path"),
        deliberation_sync=(
            source.get("deliberation_sync")
            if isinstance(source.get("deliberation_sync"), dict)
            else {}
        ),
        analysis_sync=(
            source.get("analysis_sync")
            if isinstance(source.get("analysis_sync"), dict)
            else {}
        ),
        observed_inputs=observed_inputs,
    )


def score_action(payload: dict[str, Any]) -> float:
    priority_component = priority_score(payload.get("priority"))
    pressure_component = max(
        0.0,
        min(1.0, float(payload.get("pressure_score") or 0.0)),
    ) * 2.2
    contradiction_component = min(
        1.5,
        float(payload.get("contradiction_link_count") or 0) * 0.4,
    )
    coverage_component = max(0.0, 1.0 - float(payload.get("coverage_score") or 0.0))
    confidence_value = maybe_number(payload.get("confidence"))
    uncertainty_component = (
        0.0
        if confidence_value is None
        else max(0.0, 0.9 - float(confidence_value))
    )
    blocker_component = 0.6 if bool(payload.get("readiness_blocker")) else 0.0
    probe_component = 0.45 if bool(payload.get("probe_candidate")) else 0.0
    return round(
        priority_component
        + pressure_component
        + contradiction_component
        + coverage_component
        + uncertainty_component
        + blocker_component
        + probe_component,
        3,
    )


def board_counts_from_round_state(
    round_state: dict[str, Any],
    *,
    state_source: str,
    include_notes: bool = False,
) -> dict[str, Any]:
    notes = (
        round_state.get("notes", []) if isinstance(round_state.get("notes"), list) else []
    )
    hypotheses = (
        [item for item in round_state.get("hypotheses", []) if isinstance(item, dict)]
        if isinstance(round_state.get("hypotheses"), list)
        else []
    )
    challenges = (
        [
            item
            for item in round_state.get("challenge_tickets", [])
            if isinstance(item, dict)
        ]
        if isinstance(round_state.get("challenge_tickets"), list)
        else []
    )
    tasks = (
        [item for item in round_state.get("tasks", []) if isinstance(item, dict)]
        if isinstance(round_state.get("tasks"), list)
        else []
    )
    active_hypotheses = [
        item
        for item in hypotheses
        if maybe_text(item.get("status")) not in {"closed", "rejected"}
    ]
    open_challenges = [
        item for item in challenges if maybe_text(item.get("status")) != "closed"
    ]
    open_tasks = [
        item
        for item in tasks
        if maybe_text(item.get("status")) not in {"completed", "closed", "cancelled"}
    ]
    counts = {
        "hypotheses_active": len(active_hypotheses),
        "challenge_open": len(open_challenges),
        "tasks_open": len(open_tasks),
    }
    if include_notes:
        counts["notes_total"] = len(notes)
    return {
        "state_source": state_source,
        "counts": counts,
        "active_hypotheses": active_hypotheses,
        "open_challenges": open_challenges,
        "open_tasks": open_tasks,
    }


def board_snapshot(
    round_state: dict[str, Any] | None,
    board_summary: dict[str, Any] | None,
    *,
    include_notes: bool = False,
) -> dict[str, Any]:
    if isinstance(round_state, dict):
        return board_counts_from_round_state(
            round_state,
            state_source="deliberation-plane",
            include_notes=include_notes,
        )
    if isinstance(board_summary, dict):
        counts = (
            board_summary.get("counts", {})
            if isinstance(board_summary.get("counts"), dict)
            else {}
        )
        summary_counts = {
            "hypotheses_active": int(
                counts.get("hypotheses_active")
                or len(board_summary.get("active_hypotheses", []))
            ),
            "challenge_open": int(
                counts.get("challenge_open")
                or len(board_summary.get("open_challenges", []))
            ),
            "tasks_open": int(
                counts.get("tasks_open") or len(board_summary.get("open_tasks", []))
            ),
        }
        if include_notes:
            summary_counts["notes_total"] = int(counts.get("notes_total") or 0)
        return {
            "state_source": maybe_text(board_summary.get("state_source"))
            or "board-summary-artifact",
            "counts": summary_counts,
            "active_hypotheses": (
                board_summary.get("active_hypotheses", [])
                if isinstance(board_summary.get("active_hypotheses"), list)
                else []
            ),
            "open_challenges": (
                board_summary.get("open_challenges", [])
                if isinstance(board_summary.get("open_challenges"), list)
                else []
            ),
            "open_tasks": (
                board_summary.get("open_tasks", [])
                if isinstance(board_summary.get("open_tasks"), list)
                else []
            ),
        }
    counts = {
        "hypotheses_active": 0,
        "challenge_open": 0,
        "tasks_open": 0,
    }
    if include_notes:
        counts["notes_total"] = 0
    return {
        "state_source": "missing-board",
        "counts": counts,
        "active_hypotheses": [],
        "open_challenges": [],
        "open_tasks": [],
    }


def agenda_action(
    *,
    action_id: str,
    action_kind: str,
    priority: str,
    assigned_role: str,
    objective: str,
    reason: str,
    source_ids: list[str],
    target: dict[str, Any],
    controversy_gap: str,
    recommended_lane: str,
    expected_outcome: str,
    evidence_refs: list[Any],
    probe_candidate: bool,
    contradiction_link_count: int = 0,
    coverage_score: float = 1.0,
    confidence: float | None = None,
    brief_context: str = "",
    agenda_source: str = "",
    issue_label: str = "",
    pressure_score: float = 0.5,
    readiness_blocker: bool = True,
) -> dict[str, Any]:
    return {
        "action_id": maybe_text(action_id),
        "action_kind": maybe_text(action_kind),
        "priority": maybe_text(priority) or "medium",
        "assigned_role": maybe_text(assigned_role) or "moderator",
        "objective": maybe_text(objective),
        "reason": maybe_text(reason),
        "source_ids": unique_texts(source_ids),
        "target": target if isinstance(target, dict) else {},
        "controversy_gap": maybe_text(controversy_gap),
        "recommended_lane": maybe_text(recommended_lane),
        "expected_outcome": maybe_text(expected_outcome),
        "evidence_refs": unique_texts(evidence_refs if isinstance(evidence_refs, list) else []),
        "probe_candidate": bool(probe_candidate),
        "contradiction_link_count": int(contradiction_link_count or 0),
        "coverage_score": round(float(coverage_score or 0.0), 3),
        "confidence": confidence,
        "brief_context": maybe_text(brief_context),
        "agenda_source": maybe_text(agenda_source),
        "issue_label": maybe_text(issue_label),
        "pressure_score": round(max(0.0, min(1.0, float(pressure_score or 0.0))), 3),
        "readiness_blocker": bool(readiness_blocker),
    }


def action_from_open_challenge(challenge: dict[str, Any], brief_context: str) -> dict[str, Any]:
    ticket_id = maybe_text(challenge.get("ticket_id"))
    target_claim_id = maybe_text(challenge.get("target_claim_id"))
    target_hypothesis_id = maybe_text(challenge.get("target_hypothesis_id"))
    return agenda_action(
        action_id="action-" + stable_hash("d1-action", ticket_id, "challenge")[:12],
        action_kind="resolve-challenge",
        priority=maybe_text(challenge.get("priority")) or "high",
        assigned_role=maybe_text(challenge.get("owner_role")) or "challenger",
        objective=maybe_text(challenge.get("title"))
        or "Resolve an open controversy point before closing the round.",
        reason=maybe_text(challenge.get("title"))
        or "An open contested point still needs follow-up before the controversy map is stable.",
        source_ids=[ticket_id, target_claim_id, target_hypothesis_id],
        target={
            "ticket_id": ticket_id,
            "claim_id": target_claim_id,
            "hypothesis_id": target_hypothesis_id,
        },
        controversy_gap="unresolved-contestation",
        recommended_lane="probe-before-closure",
        expected_outcome="Decide whether the contested point needs verification, rebuttal, or reframing.",
        evidence_refs=challenge.get("linked_artifact_refs", []) if isinstance(challenge.get("linked_artifact_refs"), list) else [],
        probe_candidate=True,
        contradiction_link_count=1,
        coverage_score=0.45,
        confidence=None,
        brief_context=brief_context,
        agenda_source="board-challenge",
        pressure_score=0.95,
        readiness_blocker=True,
    )


def action_from_open_task(task: dict[str, Any], brief_context: str) -> dict[str, Any]:
    task_id = maybe_text(task.get("task_id"))
    return agenda_action(
        action_id="action-" + stable_hash("d1-action", task_id, "task")[:12],
        action_kind="finish-board-task",
        priority=maybe_text(task.get("priority")) or "medium",
        assigned_role=maybe_text(task.get("owner_role")) or "moderator",
        objective=maybe_text(task.get("title"))
        or "Finish a board task that blocks controversy-map completion.",
        reason=maybe_text(task.get("title"))
        or maybe_text(task.get("task_text"))
        or "A board coordination task is still in flight.",
        source_ids=[task_id, task.get("source_ticket_id"), task.get("source_hypothesis_id")],
        target={
            "task_id": task_id,
            "ticket_id": maybe_text(task.get("source_ticket_id")),
            "hypothesis_id": maybe_text(task.get("source_hypothesis_id")),
        },
        controversy_gap="board-coordination-gap",
        recommended_lane="board-followthrough",
        expected_outcome="Finish the coordination work needed to advance the round.",
        evidence_refs=task.get("linked_artifact_refs", []) if isinstance(task.get("linked_artifact_refs"), list) else [],
        probe_candidate=False,
        contradiction_link_count=0,
        coverage_score=0.55,
        confidence=None,
        brief_context=brief_context,
        agenda_source="board-task",
        pressure_score=0.7,
        readiness_blocker=True,
    )


def action_from_hypothesis(
    hypothesis: dict[str, Any],
    brief_context: str,
) -> dict[str, Any] | None:
    confidence = maybe_number(hypothesis.get("confidence"))
    if confidence is not None and confidence >= 0.75:
        return None
    hypothesis_id = maybe_text(hypothesis.get("hypothesis_id"))
    linked_claim_ids = unique_texts(
        hypothesis.get("linked_claim_ids", [])
        if isinstance(hypothesis.get("linked_claim_ids"), list)
        else []
    )
    return agenda_action(
        action_id="action-" + stable_hash("d1-action", hypothesis_id, "hypothesis")[:12],
        action_kind="stabilize-hypothesis",
        priority="high" if (confidence or 0.0) < 0.6 else "medium",
        assigned_role=maybe_text(hypothesis.get("owner_role")) or "moderator",
        objective=maybe_text(hypothesis.get("title")) or "Stabilize an active issue interpretation.",
        reason="The board still carries an active hypothesis with limited confidence.",
        source_ids=[hypothesis_id, *linked_claim_ids],
        target={
            "hypothesis_id": hypothesis_id,
            "claim_id": linked_claim_ids[0] if linked_claim_ids else "",
        },
        controversy_gap="issue-structure-gap",
        recommended_lane="clarify-board-position",
        expected_outcome="Clarify whether the active interpretation should stay open, split, or be retired.",
        evidence_refs=[],
        probe_candidate=(confidence or 0.0) < 0.6,
        contradiction_link_count=0,
        coverage_score=0.5,
        confidence=confidence,
        brief_context=brief_context,
        agenda_source="board-hypothesis",
        pressure_score=max(0.55, 1.0 - float(confidence or 0.0)),
        readiness_blocker=True,
    )


def role_from_coverage(coverage: dict[str, Any]) -> str:
    if int(coverage.get("contradiction_link_count") or 0) > 0:
        return "challenger"
    if int(coverage.get("linked_observation_count") or 0) > 0:
        return "environmentalist"
    return "sociologist"


def action_from_coverage(
    coverage: dict[str, Any],
    brief_context: str,
) -> dict[str, Any] | None:
    readiness = maybe_text(coverage.get("readiness"))
    contradiction_count = int(coverage.get("contradiction_link_count") or 0)
    if readiness == "strong" and contradiction_count == 0:
        return None
    coverage_id = maybe_text(coverage.get("coverage_id"))
    claim_id = maybe_text(coverage.get("claim_id"))
    claim_scope_ready = bool(coverage.get("claim_scope_ready"))
    if contradiction_count > 0:
        action_kind = "resolve-contradiction"
        priority = "high"
        assigned_role = role_from_coverage(coverage)
        objective = "Check whether empirical signals materially contradict the current public narrative."
        reason = f"Claim {claim_id} has {contradiction_count} contradiction links."
        controversy_gap = "formal-public-misalignment"
        recommended_lane = "mixed-verification-review"
        expected_outcome = "Decide whether the contradiction is real, partial, or due to weak framing."
    elif not claim_scope_ready:
        action_kind = "classify-verifiability"
        priority = "high" if readiness == "weak" else "medium"
        assigned_role = "moderator"
        objective = "Clarify whether this controversy point should enter the verification lane."
        reason = (
            f"Claim {claim_id} lacks matching-ready scope and should be routed before more evidence expansion."
        )
        controversy_gap = "verification-routing-gap"
        recommended_lane = "route-before-matching"
        expected_outcome = "Classify the issue as empirical, procedural, representational, or mixed."
    else:
        action_kind = "expand-coverage"
        priority = "high" if readiness == "weak" else "medium"
        assigned_role = role_from_coverage(coverage)
        objective = "Expand verification coverage for a claim that still lacks enough support."
        reason = f"Claim {claim_id} is only {readiness or 'unknown'} on evidence coverage."
        controversy_gap = "verification-gap"
        recommended_lane = "environmental-observation"
        expected_outcome = "Add enough support to decide whether the claim should remain active."
    pressure_score = 0.64
    if contradiction_count > 0:
        pressure_score = 0.9
    elif readiness == "weak":
        pressure_score = 0.82
    elif not claim_scope_ready:
        pressure_score = 0.76
    return agenda_action(
        action_id="action-" + stable_hash("d1-action", coverage_id, action_kind)[:12],
        action_kind=action_kind,
        priority=priority,
        assigned_role=assigned_role,
        objective=objective,
        reason=reason,
        source_ids=[coverage_id, claim_id],
        target={"coverage_id": coverage_id, "claim_id": claim_id},
        controversy_gap=controversy_gap,
        recommended_lane=recommended_lane,
        expected_outcome=expected_outcome,
        evidence_refs=coverage.get("evidence_refs", []) if isinstance(coverage.get("evidence_refs"), list) else [],
        probe_candidate=contradiction_count > 0 or readiness == "weak" or action_kind == "classify-verifiability",
        contradiction_link_count=contradiction_count,
        coverage_score=float(coverage.get("coverage_score") or 0.0),
        confidence=None,
        brief_context=brief_context,
        agenda_source="evidence-coverage",
        issue_label=maybe_text(coverage.get("claim_id")),
        pressure_score=pressure_score,
        readiness_blocker=True,
    )


def prepare_promotion_action(
    coverage: dict[str, Any],
    brief_context: str,
) -> dict[str, Any]:
    coverage_id = maybe_text(coverage.get("coverage_id"))
    claim_id = maybe_text(coverage.get("claim_id"))
    return agenda_action(
        action_id="action-" + stable_hash("d1-action", coverage_id, "promotion")[:12],
        action_kind="prepare-promotion",
        priority="medium",
        assigned_role="moderator",
        objective="Prepare the round for readiness review once the controversy map is stable.",
        reason=f"Claim {claim_id} already has strong evidence coverage and can move toward readiness gating.",
        source_ids=[coverage_id, claim_id],
        target={"coverage_id": coverage_id, "claim_id": claim_id},
        controversy_gap="promotion-readiness",
        recommended_lane="promotion-review",
        expected_outcome="Freeze the strongest support path for board and reporting review.",
        evidence_refs=coverage.get("evidence_refs", []) if isinstance(coverage.get("evidence_refs"), list) else [],
        probe_candidate=False,
        contradiction_link_count=0,
        coverage_score=float(coverage.get("coverage_score") or 0.0),
        confidence=None,
        brief_context=brief_context,
        agenda_source="promotion-basis",
        issue_label=claim_id,
        pressure_score=0.25,
        readiness_blocker=False,
    )


def action_from_issue_cluster(
    issue: dict[str, Any],
    *,
    coverages_by_claim_id: dict[str, dict[str, Any]],
    links_by_issue_label: dict[str, list[dict[str, Any]]],
    gaps_by_issue_label: dict[str, list[dict[str, Any]]],
    brief_context: str,
) -> dict[str, Any] | None:
    issue_label = issue_label_for_item(issue)
    claim_ids = list_field(issue, "claim_ids")
    route_status = maybe_text(issue.get("route_status")) or "mixed-routing-review"
    lane = maybe_text(issue.get("recommended_lane")) or "mixed-review"
    weakest_coverage = weakest_coverage_for_claim_ids(claim_ids, coverages_by_claim_id)
    coverage_id = maybe_text(weakest_coverage.get("coverage_id"))
    coverage_readiness = maybe_text(weakest_coverage.get("readiness"))
    contradiction_count = int(weakest_coverage.get("contradiction_link_count") or 0)
    link_rows = links_by_issue_label.get(issue_label, [])
    link_status = maybe_text(link_rows[0].get("link_status")) if link_rows else "unlinked"
    gap_rows = gaps_by_issue_label.get(issue_label, [])
    evidence_refs = unique_texts(
        list(issue.get("evidence_refs", []) if isinstance(issue.get("evidence_refs"), list) else [])
        + list(weakest_coverage.get("evidence_refs", []) if isinstance(weakest_coverage.get("evidence_refs"), list) else [])
    )
    source_ids = unique_texts(
        [issue.get("map_issue_id"), issue.get("cluster_id"), coverage_id, *claim_ids]
    )
    base_target = {
        "map_issue_id": maybe_text(issue.get("map_issue_id")),
        "cluster_id": maybe_text(issue.get("cluster_id")),
        "claim_id": claim_ids[0] if claim_ids else "",
        "coverage_id": coverage_id,
    }
    if route_status == "mixed-routing-review" or lane in {"mixed-review", "route-before-matching"}:
        return agenda_action(
            action_id="action-" + stable_hash("d1-agenda", issue.get("map_issue_id"), "route")[:12],
            action_kind="clarify-verification-route",
            priority="high",
            assigned_role="moderator",
            objective=f"Clarify which investigation lane should govern {issue_label}.",
            reason=maybe_text(issue.get("controversy_summary"))
            or f"Issue {issue_label} is still routed ambiguously and should not be advanced through a single lane yet.",
            source_ids=source_ids,
            target=base_target,
            controversy_gap="verification-routing-gap",
            recommended_lane=lane,
            expected_outcome="Decide whether the issue belongs in empirical verification, formal record review, discourse analysis, or mixed review.",
            evidence_refs=evidence_refs,
            probe_candidate=True,
            contradiction_link_count=contradiction_count,
            coverage_score=float(weakest_coverage.get("coverage_score") or 1.0) if weakest_coverage else 1.0,
            confidence=None,
            brief_context=brief_context,
            agenda_source="controversy-map",
            issue_label=issue_label,
            pressure_score=0.9,
            readiness_blocker=True,
        )
    if maybe_text(issue.get("controversy_posture")) == "empirical-issue" or lane == "environmental-observation":
        if not weakest_coverage or contradiction_count > 0 or coverage_readiness != "strong":
            coverage_score = float(weakest_coverage.get("coverage_score") or 0.0) if weakest_coverage else 0.0
            if contradiction_count > 0:
                controversy_gap = "formal-public-misalignment"
                reason = f"Issue {issue_label} still carries {contradiction_count} contradiction links against its empirical support path."
                pressure_score = 0.92
            else:
                controversy_gap = "verification-gap"
                reason = (
                    f"Issue {issue_label} is only {coverage_readiness or 'unscored'} on empirical evidence coverage."
                    if weakest_coverage
                    else f"Issue {issue_label} has not yet been grounded in a usable evidence-coverage object."
                )
                pressure_score = 0.84 if not weakest_coverage or coverage_readiness == "weak" else 0.66
            return agenda_action(
                action_id="action-" + stable_hash("d1-agenda", issue.get("map_issue_id"), "empirical")[:12],
                action_kind="advance-empirical-verification",
                priority="high" if pressure_score >= 0.8 else "medium",
                assigned_role=role_from_coverage(weakest_coverage) if weakest_coverage else "environmentalist",
                objective=f"Advance empirical verification for {issue_label}.",
                reason=reason,
                source_ids=source_ids,
                target=base_target,
                controversy_gap=controversy_gap,
                recommended_lane="environmental-observation",
                expected_outcome="Decide whether available environmental observations materially support, weaken, or reframe the issue.",
                evidence_refs=evidence_refs,
                probe_candidate=contradiction_count > 0 or coverage_readiness in {"", "weak"},
                contradiction_link_count=contradiction_count,
                coverage_score=coverage_score,
                confidence=None,
                brief_context=brief_context,
                agenda_source="controversy-map",
                issue_label=issue_label,
                pressure_score=pressure_score,
                readiness_blocker=True,
            )
        return None
    if gap_rows:
        return None
    if lane == "formal-comment-and-policy-record" and link_status in {"unlinked", "public-only"}:
        return agenda_action(
            action_id="action-" + stable_hash("d1-agenda", issue.get("map_issue_id"), "formal-record")[:12],
            action_kind="review-formal-record",
            priority="high" if link_status == "public-only" else "medium",
            assigned_role="moderator",
            objective=f"Check whether the formal record sufficiently represents {issue_label}.",
            reason=f"Issue {issue_label} is routed into formal record review but the current linkage posture is {link_status}.",
            source_ids=source_ids,
            target=base_target,
            controversy_gap="formal-record-gap",
            recommended_lane=lane,
            expected_outcome="Establish whether the formal policy record captures the issue strongly enough to close the round.",
            evidence_refs=evidence_refs,
            probe_candidate=link_status == "public-only",
            contradiction_link_count=0,
            coverage_score=1.0,
            confidence=None,
            brief_context=brief_context,
            agenda_source="controversy-map",
            issue_label=issue_label,
            pressure_score=0.78 if link_status == "public-only" else 0.6,
            readiness_blocker=True,
        )
    if lane == "public-discourse-analysis" and link_status in {"unlinked", "formal-only"}:
        return agenda_action(
            action_id="action-" + stable_hash("d1-agenda", issue.get("map_issue_id"), "public-discourse")[:12],
            action_kind="analyze-public-discourse",
            priority="high" if link_status == "formal-only" else "medium",
            assigned_role="sociologist",
            objective=f"Check whether public discourse around {issue_label} is adequately represented and interpretable.",
            reason=f"Issue {issue_label} stays in discourse analysis but the current linkage posture is {link_status}.",
            source_ids=source_ids,
            target=base_target,
            controversy_gap="public-discourse-gap",
            recommended_lane=lane,
            expected_outcome="Decide whether discourse-side evidence is representative enough to stabilize the issue.",
            evidence_refs=evidence_refs,
            probe_candidate=link_status == "formal-only",
            contradiction_link_count=0,
            coverage_score=1.0,
            confidence=None,
            brief_context=brief_context,
            agenda_source="controversy-map",
            issue_label=issue_label,
            pressure_score=0.76 if link_status == "formal-only" else 0.58,
            readiness_blocker=True,
        )
    if lane == "stakeholder-deliberation-analysis" and link_status in {"unlinked", "formal-only"}:
        return agenda_action(
            action_id="action-" + stable_hash("d1-agenda", issue.get("map_issue_id"), "stakeholder")[:12],
            action_kind="analyze-stakeholder-deliberation",
            priority="medium",
            assigned_role="sociologist",
            objective=f"Check whether stakeholder positions around {issue_label} remain underrepresented.",
            reason=f"Issue {issue_label} is staying in stakeholder-deliberation analysis while linkage posture is {link_status}.",
            source_ids=source_ids,
            target=base_target,
            controversy_gap="stakeholder-deliberation-gap",
            recommended_lane=lane,
            expected_outcome="Clarify whether stakeholder positions need more explicit representation before the round can close.",
            evidence_refs=evidence_refs,
            probe_candidate=False,
            contradiction_link_count=0,
            coverage_score=1.0,
            confidence=None,
            brief_context=brief_context,
            agenda_source="controversy-map",
            issue_label=issue_label,
            pressure_score=0.58,
            readiness_blocker=True,
        )
    return None


def action_from_verification_route(
    route: dict[str, Any],
    *,
    coverages_by_claim_id: dict[str, dict[str, Any]],
    links_by_issue_label: dict[str, list[dict[str, Any]]],
    brief_context: str,
) -> dict[str, Any] | None:
    claim_id = maybe_text(route.get("claim_id"))
    lane = maybe_text(route.get("recommended_lane")) or "mixed-review"
    route_status = maybe_text(route.get("route_status")) or "mixed-routing-review"
    issue_label = issue_label_for_item(route)
    source_ids = unique_texts([route.get("route_id"), claim_id, route.get("assessment_id")])
    evidence_refs = route.get("evidence_refs", []) if isinstance(route.get("evidence_refs"), list) else []
    weakest_coverage = weakest_coverage_for_claim_ids([claim_id], coverages_by_claim_id)
    if route_status == "mixed-routing-review" or lane in {"mixed-review", "route-before-matching"}:
        return agenda_action(
            action_id="action-" + stable_hash("d1-route", route.get("route_id"), "route")[:12],
            action_kind="clarify-verification-route",
            priority="high",
            assigned_role="moderator",
            objective=f"Clarify which lane should govern {issue_label}.",
            reason=maybe_text(route.get("route_reason"))
            or f"Claim {claim_id or issue_label} is still under mixed routing review.",
            source_ids=source_ids,
            target={"route_id": maybe_text(route.get("route_id")), "claim_id": claim_id},
            controversy_gap="verification-routing-gap",
            recommended_lane=lane,
            expected_outcome="Freeze a single routing posture before more downstream work is queued.",
            evidence_refs=evidence_refs,
            probe_candidate=True,
            contradiction_link_count=0,
            coverage_score=float(weakest_coverage.get("coverage_score") or 1.0) if weakest_coverage else 1.0,
            confidence=maybe_number(route.get("confidence")),
            brief_context=brief_context,
            agenda_source="verification-route",
            issue_label=issue_label,
            pressure_score=0.88,
            readiness_blocker=True,
        )
    if lane == "environmental-observation":
        coverage_readiness = maybe_text(weakest_coverage.get("readiness"))
        contradiction_count = int(weakest_coverage.get("contradiction_link_count") or 0)
        if not weakest_coverage or contradiction_count > 0 or coverage_readiness != "strong":
            return agenda_action(
                action_id="action-" + stable_hash("d1-route", route.get("route_id"), "empirical")[:12],
                action_kind="advance-empirical-verification",
                priority="high",
                assigned_role=role_from_coverage(weakest_coverage) if weakest_coverage else "environmentalist",
                objective=f"Advance empirical verification for {issue_label}.",
                reason=maybe_text(route.get("route_reason"))
                or f"Claim {claim_id or issue_label} is routed to environmental observation but its evidence basis is not yet stable.",
                source_ids=source_ids + unique_texts([weakest_coverage.get("coverage_id")]),
                target={
                    "route_id": maybe_text(route.get("route_id")),
                    "claim_id": claim_id,
                    "coverage_id": maybe_text(weakest_coverage.get("coverage_id")),
                },
                controversy_gap="formal-public-misalignment" if contradiction_count > 0 else "verification-gap",
                recommended_lane=lane,
                expected_outcome="Decide whether the route should stay empirical and whether the issue can be stabilized with available signals.",
                evidence_refs=evidence_refs + list(weakest_coverage.get("evidence_refs", []) if isinstance(weakest_coverage.get("evidence_refs"), list) else []),
                probe_candidate=contradiction_count > 0 or coverage_readiness in {"", "weak"},
                contradiction_link_count=contradiction_count,
                coverage_score=float(weakest_coverage.get("coverage_score") or 0.0) if weakest_coverage else 0.0,
                confidence=maybe_number(route.get("confidence")),
                brief_context=brief_context,
                agenda_source="verification-route",
                issue_label=issue_label,
                pressure_score=0.84 if contradiction_count == 0 else 0.9,
                readiness_blocker=True,
            )
        return None
    link_rows = links_by_issue_label.get(issue_label, [])
    link_status = maybe_text(link_rows[0].get("link_status")) if link_rows else "unlinked"
    if lane == "formal-comment-and-policy-record" and link_status in {"unlinked", "public-only"}:
        return agenda_action(
            action_id="action-" + stable_hash("d1-route", route.get("route_id"), "formal")[:12],
            action_kind="review-formal-record",
            priority="high" if link_status == "public-only" else "medium",
            assigned_role="moderator",
            objective=f"Review the formal record posture for {issue_label}.",
            reason=maybe_text(route.get("route_reason"))
            or f"Claim {claim_id or issue_label} is routed to formal record review but linkage posture is {link_status}.",
            source_ids=source_ids,
            target={"route_id": maybe_text(route.get("route_id")), "claim_id": claim_id},
            controversy_gap="formal-record-gap",
            recommended_lane=lane,
            expected_outcome="Decide whether formal record material is enough to represent the issue cleanly.",
            evidence_refs=evidence_refs,
            probe_candidate=link_status == "public-only",
            contradiction_link_count=0,
            coverage_score=1.0,
            confidence=maybe_number(route.get("confidence")),
            brief_context=brief_context,
            agenda_source="verification-route",
            issue_label=issue_label,
            pressure_score=0.74 if link_status == "public-only" else 0.56,
            readiness_blocker=True,
        )
    if lane in {"public-discourse-analysis", "stakeholder-deliberation-analysis"} and link_status in {"unlinked", "formal-only"}:
        action_kind = "analyze-public-discourse" if lane == "public-discourse-analysis" else "analyze-stakeholder-deliberation"
        controversy_gap = "public-discourse-gap" if lane == "public-discourse-analysis" else "stakeholder-deliberation-gap"
        return agenda_action(
            action_id="action-" + stable_hash("d1-route", route.get("route_id"), action_kind)[:12],
            action_kind=action_kind,
            priority="high" if link_status == "formal-only" else "medium",
            assigned_role="sociologist",
            objective=f"Review the discourse-side representation posture for {issue_label}.",
            reason=maybe_text(route.get("route_reason"))
            or f"Claim {claim_id or issue_label} stays in discourse-side review but linkage posture is {link_status}.",
            source_ids=source_ids,
            target={"route_id": maybe_text(route.get("route_id")), "claim_id": claim_id},
            controversy_gap=controversy_gap,
            recommended_lane=lane,
            expected_outcome="Decide whether discourse or stakeholder representation is sufficient to stabilize the issue.",
            evidence_refs=evidence_refs,
            probe_candidate=link_status == "formal-only",
            contradiction_link_count=0,
            coverage_score=1.0,
            confidence=maybe_number(route.get("confidence")),
            brief_context=brief_context,
            agenda_source="verification-route",
            issue_label=issue_label,
            pressure_score=0.72 if link_status == "formal-only" else 0.55,
            readiness_blocker=True,
        )
    return None


def action_from_claim_assessment(
    assessment: dict[str, Any],
    *,
    coverages_by_claim_id: dict[str, dict[str, Any]],
    brief_context: str,
) -> dict[str, Any] | None:
    claim_id = maybe_text(assessment.get("claim_id"))
    lane = maybe_text(assessment.get("recommended_lane")) or "mixed-review"
    issue_label = issue_label_for_item(assessment)
    route_ready = bool(assessment.get("route_to_observation_matching"))
    risk_flags = list_field(assessment, "risk_flags")
    weakest_coverage = weakest_coverage_for_claim_ids([claim_id], coverages_by_claim_id)
    source_ids = unique_texts([assessment.get("assessment_id"), claim_id, assessment.get("claim_scope_id")])
    evidence_refs = assessment.get("evidence_refs", []) if isinstance(assessment.get("evidence_refs"), list) else []
    if lane in {"mixed-review", "route-before-matching"} or not route_ready:
        return agenda_action(
            action_id="action-" + stable_hash("d1-assessment", assessment.get("assessment_id"), "route")[:12],
            action_kind="clarify-verification-route",
            priority="high",
            assigned_role="moderator",
            objective=f"Clarify which lane should govern {issue_label}.",
            reason=maybe_text(assessment.get("assessment_summary"))
            or f"Claim {claim_id or issue_label} is not yet matching-ready and still needs routing clarification.",
            source_ids=source_ids,
            target={"assessment_id": maybe_text(assessment.get("assessment_id")), "claim_id": claim_id},
            controversy_gap="verification-routing-gap",
            recommended_lane=lane,
            expected_outcome="Freeze a routing decision before more downstream evidence work is queued.",
            evidence_refs=evidence_refs,
            probe_candidate=True,
            contradiction_link_count=0,
            coverage_score=float(weakest_coverage.get("coverage_score") or 1.0) if weakest_coverage else 1.0,
            confidence=maybe_number(assessment.get("confidence")),
            brief_context=brief_context,
            agenda_source="claim-verifiability",
            issue_label=issue_label,
            pressure_score=0.86 if "not-matching-ready" in risk_flags else 0.76,
            readiness_blocker=True,
        )
    if lane == "environmental-observation":
        coverage_readiness = maybe_text(weakest_coverage.get("readiness"))
        contradiction_count = int(weakest_coverage.get("contradiction_link_count") or 0)
        if not weakest_coverage or contradiction_count > 0 or coverage_readiness != "strong":
            return agenda_action(
                action_id="action-" + stable_hash("d1-assessment", assessment.get("assessment_id"), "empirical")[:12],
                action_kind="advance-empirical-verification",
                priority="high",
                assigned_role=role_from_coverage(weakest_coverage) if weakest_coverage else "environmentalist",
                objective=f"Advance empirical verification for {issue_label}.",
                reason=maybe_text(assessment.get("assessment_summary"))
                or f"Claim {claim_id or issue_label} is empirical but its evidence basis is not yet stable.",
                source_ids=source_ids + unique_texts([weakest_coverage.get("coverage_id")]),
                target={
                    "assessment_id": maybe_text(assessment.get("assessment_id")),
                    "claim_id": claim_id,
                    "coverage_id": maybe_text(weakest_coverage.get("coverage_id")),
                },
                controversy_gap="formal-public-misalignment" if contradiction_count > 0 else "verification-gap",
                recommended_lane=lane,
                expected_outcome="Decide whether the claim can be grounded in environmental observations strongly enough to stabilize the issue.",
                evidence_refs=evidence_refs + list(weakest_coverage.get("evidence_refs", []) if isinstance(weakest_coverage.get("evidence_refs"), list) else []),
                probe_candidate=contradiction_count > 0 or coverage_readiness in {"", "weak"},
                contradiction_link_count=contradiction_count,
                coverage_score=float(weakest_coverage.get("coverage_score") or 0.0) if weakest_coverage else 0.0,
                confidence=maybe_number(assessment.get("confidence")),
                brief_context=brief_context,
                agenda_source="claim-verifiability",
                issue_label=issue_label,
                pressure_score=0.82 if contradiction_count == 0 else 0.9,
                readiness_blocker=True,
            )
        return None
    action_kind = "review-formal-record"
    controversy_gap = "formal-record-gap"
    assigned_role = "moderator"
    if lane == "public-discourse-analysis":
        action_kind = "analyze-public-discourse"
        controversy_gap = "public-discourse-gap"
        assigned_role = "sociologist"
    elif lane == "stakeholder-deliberation-analysis":
        action_kind = "analyze-stakeholder-deliberation"
        controversy_gap = "stakeholder-deliberation-gap"
        assigned_role = "sociologist"
    return agenda_action(
        action_id="action-" + stable_hash("d1-assessment", assessment.get("assessment_id"), action_kind)[:12],
        action_kind=action_kind,
        priority="medium",
        assigned_role=assigned_role,
        objective=f"Advance the {lane.replace('-', ' ')} posture for {issue_label}.",
        reason=maybe_text(assessment.get("assessment_summary"))
        or f"Claim {claim_id or issue_label} is classified into the {lane.replace('-', ' ')} lane.",
        source_ids=source_ids,
        target={"assessment_id": maybe_text(assessment.get("assessment_id")), "claim_id": claim_id},
        controversy_gap=controversy_gap,
        recommended_lane=lane,
        expected_outcome="Decide whether the issue has enough non-empirical structure to stabilize without further rerouting.",
        evidence_refs=evidence_refs,
        probe_candidate=False,
        contradiction_link_count=0,
        coverage_score=1.0,
        confidence=maybe_number(assessment.get("confidence")),
        brief_context=brief_context,
        agenda_source="claim-verifiability",
        issue_label=issue_label,
        pressure_score=0.58,
        readiness_blocker=True,
    )


def action_from_formal_public_link(
    link: dict[str, Any],
    *,
    brief_context: str,
) -> dict[str, Any] | None:
    link_status = maybe_text(link.get("link_status"))
    if link_status in {"", "aligned"}:
        return None
    issue_label = issue_label_for_item(link)
    linkage_id = maybe_text(link.get("linkage_id"))
    lane = maybe_text(link.get("recommended_lane")) or "mixed-review"
    formal_count = int(link.get("formal_signal_count") or 0)
    public_count = int(link.get("public_signal_count") or 0)
    pressure_score = max(0.56, 1.0 - float(link.get("alignment_score") or 0.0))
    return agenda_action(
        action_id="action-" + stable_hash("d1-link", linkage_id, link_status)[:12],
        action_kind="review-formal-public-linkage",
        priority=priority_from_score(pressure_score),
        assigned_role=role_from_lane(lane, default_role="moderator"),
        objective=f"Review how formal and public material are linked for {issue_label}.",
        reason=maybe_text(link.get("linkage_summary"))
        or f"Issue {issue_label} currently has a {link_status} formal/public linkage posture.",
        source_ids=[linkage_id, *list_field(link, "claim_ids"), *list_field(link, "cluster_ids")],
        target={"linkage_id": linkage_id, "issue_label": issue_label},
        controversy_gap="formal-public-linkage-gap",
        recommended_lane=lane,
        expected_outcome="Decide whether the issue still lacks enough formal or public representation to move forward cleanly.",
        evidence_refs=link.get("evidence_refs", []) if isinstance(link.get("evidence_refs"), list) else [],
        probe_candidate=link_status in {"public-only", "formal-only"},
        contradiction_link_count=0,
        coverage_score=1.0,
        confidence=None,
        brief_context=brief_context,
        agenda_source="formal-public-link",
        issue_label=issue_label,
        pressure_score=pressure_score,
        readiness_blocker=True,
    )


def action_from_representation_gap(
    gap: dict[str, Any],
    *,
    brief_context: str,
) -> dict[str, Any]:
    issue_label = issue_label_for_item(gap)
    gap_id = maybe_text(gap.get("gap_id"))
    severity_score = float(gap.get("severity_score") or 0.0)
    return agenda_action(
        action_id="action-" + stable_hash("d1-gap", gap_id, gap.get("gap_type"))[:12],
        action_kind="address-representation-gap",
        priority=maybe_text(gap.get("severity")) or priority_from_score(severity_score),
        assigned_role="sociologist",
        objective=f"Address the representation gap around {issue_label}.",
        reason=maybe_text(gap.get("gap_summary"))
        or maybe_text(gap.get("recommended_action"))
        or f"Issue {issue_label} currently carries a representation gap.",
        source_ids=[gap_id, gap.get("linkage_id"), *list_field(gap, "claim_ids"), *list_field(gap, "cluster_ids")],
        target={"gap_id": gap_id, "linkage_id": maybe_text(gap.get("linkage_id"))},
        controversy_gap="representation-gap",
        recommended_lane=maybe_text(gap.get("recommended_lane")) or "public-discourse-analysis",
        expected_outcome=maybe_text(gap.get("recommended_action"))
        or "Decide how the round should repair the missing or imbalanced representation posture.",
        evidence_refs=gap.get("evidence_refs", []) if isinstance(gap.get("evidence_refs"), list) else [],
        probe_candidate=severity_score >= 0.72,
        contradiction_link_count=0,
        coverage_score=1.0,
        confidence=None,
        brief_context=brief_context,
        agenda_source="representation-gap",
        issue_label=issue_label,
        pressure_score=max(0.58, severity_score),
        readiness_blocker=True,
    )


def action_from_diffusion_edge(
    edge: dict[str, Any],
    *,
    brief_context: str,
) -> dict[str, Any] | None:
    edge_type = maybe_text(edge.get("edge_type"))
    confidence = maybe_number(edge.get("confidence")) or 0.0
    if edge_type not in {
        "public-to-formal-spillover",
        "formal-to-public-spillover",
        "cross-public-diffusion",
    }:
        return None
    if confidence < 0.72:
        return None
    issue_label = issue_label_for_item(edge)
    edge_id = maybe_text(edge.get("edge_id"))
    priority = "high" if edge_type in {"public-to-formal-spillover", "formal-to-public-spillover"} and confidence >= 0.8 else "medium"
    return agenda_action(
        action_id="action-" + stable_hash("d1-diffusion", edge_id, edge_type)[:12],
        action_kind="trace-cross-platform-diffusion",
        priority=priority,
        assigned_role="sociologist",
        objective=f"Trace how {issue_label} is moving across platforms.",
        reason=maybe_text(edge.get("edge_summary"))
        or f"Issue {issue_label} shows a {edge_type} pattern that may affect how the controversy is represented.",
        source_ids=[edge_id, *list_field(edge, "claim_ids"), *list_field(edge, "cluster_ids"), *list_field(edge, "source_signal_ids"), *list_field(edge, "target_signal_ids")],
        target={
            "edge_id": edge_id,
            "source_platform": maybe_text(edge.get("source_platform")),
            "target_platform": maybe_text(edge.get("target_platform")),
        },
        controversy_gap="cross-platform-diffusion",
        recommended_lane=maybe_text(edge.get("recommended_lane")) or "public-discourse-analysis",
        expected_outcome="Decide whether diffusion across platforms materially changes how the issue should be represented or handed off.",
        evidence_refs=edge.get("evidence_refs", []) if isinstance(edge.get("evidence_refs"), list) else [],
        probe_candidate=False,
        contradiction_link_count=0,
        coverage_score=1.0,
        confidence=confidence,
        brief_context=brief_context,
        agenda_source="diffusion-edge",
        issue_label=issue_label,
        pressure_score=min(0.86, confidence),
        readiness_blocker=False,
    )


def build_actions(
    board_state: dict[str, Any],
    coverages: list[dict[str, Any]],
    issue_clusters: list[dict[str, Any]],
    routes: list[dict[str, Any]],
    assessments: list[dict[str, Any]],
    links: list[dict[str, Any]],
    gaps: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    brief_text: str,
) -> list[dict[str, Any]]:
    brief_context = excerpt_text(brief_text)
    actions: list[dict[str, Any]] = []
    coverages_by_claim_id = indexed_by_claim_id(coverages)
    links_by_issue_label = grouped_by_issue_label(links)
    gaps_by_issue_label = grouped_by_issue_label(gaps)
    for challenge in board_state.get("open_challenges", []):
        if isinstance(challenge, dict):
            actions.append(action_from_open_challenge(challenge, brief_context))
    for task in board_state.get("open_tasks", []):
        if isinstance(task, dict):
            actions.append(action_from_open_task(task, brief_context))
    for hypothesis in board_state.get("active_hypotheses", []):
        if isinstance(hypothesis, dict):
            candidate = action_from_hypothesis(hypothesis, brief_context)
            if candidate is not None:
                actions.append(candidate)
    for issue in issue_clusters:
        if isinstance(issue, dict):
            candidate = action_from_issue_cluster(
                issue,
                coverages_by_claim_id=coverages_by_claim_id,
                links_by_issue_label=links_by_issue_label,
                gaps_by_issue_label=gaps_by_issue_label,
                brief_context=brief_context,
            )
            if candidate is not None:
                actions.append(candidate)
    if not issue_clusters:
        for route in routes:
            if isinstance(route, dict):
                candidate = action_from_verification_route(
                    route,
                    coverages_by_claim_id=coverages_by_claim_id,
                    links_by_issue_label=links_by_issue_label,
                    brief_context=brief_context,
                )
                if candidate is not None:
                    actions.append(candidate)
    if not issue_clusters and not routes:
        for assessment in assessments:
            if isinstance(assessment, dict):
                candidate = action_from_claim_assessment(
                    assessment,
                    coverages_by_claim_id=coverages_by_claim_id,
                    brief_context=brief_context,
                )
                if candidate is not None:
                    actions.append(candidate)
    for gap in gaps:
        if isinstance(gap, dict):
            actions.append(action_from_representation_gap(gap, brief_context=brief_context))
    if not gaps:
        for link in links:
            if isinstance(link, dict):
                candidate = action_from_formal_public_link(link, brief_context=brief_context)
                if candidate is not None:
                    actions.append(candidate)
    for edge in edges:
        if isinstance(edge, dict):
            candidate = action_from_diffusion_edge(edge, brief_context=brief_context)
            if candidate is not None:
                actions.append(candidate)
    if not (issue_clusters or routes or assessments or links or gaps or edges):
        for coverage in coverages:
            if isinstance(coverage, dict):
                candidate = action_from_coverage(coverage, brief_context)
                if candidate is not None:
                    actions.append(candidate)
    if not actions:
        strong_coverages = [
            coverage
            for coverage in coverages
            if isinstance(coverage, dict)
            and maybe_text(coverage.get("readiness")) == "strong"
        ]
        if strong_coverages:
            actions.append(prepare_promotion_action(strong_coverages[0], brief_context))
    deduped: dict[str, dict[str, Any]] = {}
    for action in actions:
        key = "|".join(
            unique_texts([action.get("action_kind"), *(action.get("source_ids") or [])])
        )
        if key in deduped:
            continue
        deduped[key] = action
    ranked = list(deduped.values())
    for action in ranked:
        action["score"] = score_action(action)
    ranked.sort(
        key=lambda item: (
            -float(item.get("score") or 0.0),
            -priority_score(item.get("priority")),
            maybe_text(item.get("action_id")),
        )
    )
    for index, action in enumerate(ranked, start=1):
        action["rank"] = index
    return ranked


def primary_analysis_sync(
    contexts: dict[str, tuple[dict[str, Any], str]],
    *,
    fallback: dict[str, Any] | None = None,
) -> dict[str, Any]:
    selected_name = ""
    selected_sync = fallback if isinstance(fallback, dict) else {}
    available_analysis_kinds: list[str] = []
    context_item_counts: dict[str, int] = {}
    for name, (context, count_key) in contexts.items():
        count = optional_context_count(context, count_key)
        context_item_counts[name] = count
        if optional_context_present(context, count_key):
            available_analysis_kinds.append(name)
            if not selected_name and isinstance(context.get("analysis_sync"), dict):
                selected_name = name
                selected_sync = context.get("analysis_sync", {})
    if not isinstance(selected_sync, dict):
        selected_sync = {}
    return {
        **selected_sync,
        "selected_analysis_kind": selected_name,
        "available_analysis_kinds": available_analysis_kinds,
        "context_item_counts": context_item_counts,
    }


def controversy_context_counts(
    *,
    issue_clusters: list[dict[str, Any]],
    routes: list[dict[str, Any]],
    links: list[dict[str, Any]],
    gaps: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    coverages: list[dict[str, Any]],
) -> dict[str, int]:
    coverages_by_claim_id = indexed_by_claim_id(coverages)
    empirical_issue_count = 0
    non_empirical_issue_count = 0
    mixed_issue_count = 0
    routing_issue_count = 0
    empirical_issue_gap_count = 0
    formal_public_linkage_gap_count = 0
    for issue in issue_clusters:
        if not isinstance(issue, dict):
            continue
        posture = maybe_text(issue.get("controversy_posture"))
        lane = maybe_text(issue.get("recommended_lane"))
        route_status = maybe_text(issue.get("route_status")) or "mixed-routing-review"
        claim_ids = list_field(issue, "claim_ids")
        weakest_coverage = weakest_coverage_for_claim_ids(claim_ids, coverages_by_claim_id)
        if posture == "empirical-issue" or lane == "environmental-observation":
            empirical_issue_count += 1
            if (
                not weakest_coverage
                or maybe_text(weakest_coverage.get("readiness")) != "strong"
                or int(weakest_coverage.get("contradiction_link_count") or 0) > 0
            ):
                empirical_issue_gap_count += 1
        elif posture == "non-empirical-issue":
            non_empirical_issue_count += 1
        elif posture:
            mixed_issue_count += 1
        if route_status == "mixed-routing-review" or lane in {"mixed-review", "route-before-matching"}:
            routing_issue_count += 1
    if not issue_clusters:
        for route in routes:
            if not isinstance(route, dict):
                continue
            lane = maybe_text(route.get("recommended_lane"))
            route_status = maybe_text(route.get("route_status")) or "mixed-routing-review"
            claim_id = maybe_text(route.get("claim_id"))
            weakest_coverage = weakest_coverage_for_claim_ids([claim_id], coverages_by_claim_id)
            if lane == "environmental-observation":
                empirical_issue_count += 1
                if (
                    not weakest_coverage
                    or maybe_text(weakest_coverage.get("readiness")) != "strong"
                    or int(weakest_coverage.get("contradiction_link_count") or 0) > 0
                ):
                    empirical_issue_gap_count += 1
            elif lane in {
                "formal-comment-and-policy-record",
                "public-discourse-analysis",
                "stakeholder-deliberation-analysis",
            }:
                non_empirical_issue_count += 1
            else:
                mixed_issue_count += 1
            if route_status == "mixed-routing-review" or lane in {"mixed-review", "route-before-matching"}:
                routing_issue_count += 1
    for link in links:
        if not isinstance(link, dict):
            continue
        if maybe_text(link.get("link_status")) not in {"", "aligned"}:
            formal_public_linkage_gap_count += 1
    diffusion_focus_count = 0
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        edge_type = maybe_text(edge.get("edge_type"))
        confidence = maybe_number(edge.get("confidence")) or 0.0
        if edge_type in {
            "public-to-formal-spillover",
            "formal-to-public-spillover",
            "cross-public-diffusion",
        } and confidence >= 0.72:
            diffusion_focus_count += 1
    return {
        "issue_cluster_count": len([item for item in issue_clusters if isinstance(item, dict)]),
        "empirical_issue_count": empirical_issue_count,
        "non_empirical_issue_count": non_empirical_issue_count,
        "mixed_issue_count": mixed_issue_count,
        "routing_issue_count": routing_issue_count,
        "empirical_issue_gap_count": empirical_issue_gap_count,
        "representation_gap_count": len([item for item in gaps if isinstance(item, dict)]),
        "formal_public_linkage_gap_count": formal_public_linkage_gap_count,
        "diffusion_focus_count": diffusion_focus_count,
    }


def load_d1_shared_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    board_summary_path: str = "",
    board_brief_path: str = "",
    coverage_path: str = "",
    include_board_notes: bool = False,
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    board_summary_file = resolve_path(
        run_dir_path,
        board_summary_path,
        f"board/board_state_summary_{round_id}.json",
    )
    board_brief_file = resolve_path(
        run_dir_path,
        board_brief_path,
        f"board/board_brief_{round_id}.md",
    )

    warnings: list[dict[str, Any]] = []
    round_snapshot = load_round_snapshot(
        run_dir_path,
        expected_run_id=run_id,
        round_id=round_id,
        include_closed=True,
    )
    board_summary = load_json_if_exists(board_summary_file)
    round_state = (
        round_snapshot.get("round_state")
        if maybe_text(round_snapshot.get("status")) == "completed"
        and isinstance(round_snapshot.get("round_state"), dict)
        else None
    )
    if round_state is None and not isinstance(board_summary, dict):
        warnings.append(
            {
                "code": "missing-board-state",
                "message": f"No board state was found for round {round_id}.",
            }
        )
    deliberation_sync = (
        round_snapshot.get("deliberation_sync")
        if isinstance(round_snapshot.get("deliberation_sync"), dict)
        else {}
    )
    db_path = maybe_text(round_snapshot.get("db_path"))
    coverage_context = load_evidence_coverage_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        coverage_path=coverage_path,
        db_path=db_path,
    )
    coverage_warnings = (
        coverage_context.get("warnings", [])
        if isinstance(coverage_context.get("warnings"), list)
        else []
    )
    warnings.extend(coverage_warnings)
    coverages = (
        coverage_context.get("coverages", [])
        if isinstance(coverage_context.get("coverages"), list)
        else []
    )
    coverage_file = maybe_text(coverage_context.get("coverage_file"))
    coverage_source = maybe_text(coverage_context.get("coverage_source"))
    if not db_path:
        db_path = maybe_text(coverage_context.get("db_path"))
    controversy_map_context = load_controversy_map_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    )
    if not db_path:
        db_path = maybe_text(controversy_map_context.get("db_path"))
    verification_route_context = load_verification_route_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    )
    claim_verifiability_context = load_claim_verifiability_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    )
    formal_public_link_context = load_formal_public_link_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    )
    representation_gap_context = load_representation_gap_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    )
    diffusion_edge_context = load_diffusion_edge_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        db_path=db_path,
    )
    warnings.extend(optional_context_warnings(controversy_map_context))
    warnings.extend(optional_context_warnings(verification_route_context))
    warnings.extend(optional_context_warnings(claim_verifiability_context))
    warnings.extend(optional_context_warnings(formal_public_link_context))
    warnings.extend(optional_context_warnings(representation_gap_context))
    warnings.extend(optional_context_warnings(diffusion_edge_context))
    analysis_sync = primary_analysis_sync(
        {
            "controversy-map": (controversy_map_context, "issue_cluster_count"),
            "verification-route": (verification_route_context, "route_count"),
            "claim-verifiability": (claim_verifiability_context, "assessment_count"),
            "formal-public-link": (formal_public_link_context, "link_count"),
            "representation-gap": (representation_gap_context, "gap_count"),
            "diffusion-edge": (diffusion_edge_context, "edge_count"),
            "evidence-coverage": (coverage_context, "coverage_count"),
        },
        fallback=coverage_context.get("analysis_sync") if isinstance(coverage_context.get("analysis_sync"), dict) else {},
    )
    brief_text = load_text_if_exists(board_brief_file)
    current_board_state = board_snapshot(
        round_state,
        board_summary if isinstance(board_summary, dict) else None,
        include_notes=include_board_notes,
    )
    issue_clusters = (
        controversy_map_context.get("issue_clusters", [])
        if isinstance(controversy_map_context.get("issue_clusters"), list)
        else []
    )
    routes = (
        verification_route_context.get("routes", [])
        if isinstance(verification_route_context.get("routes"), list)
        else []
    )
    assessments = (
        claim_verifiability_context.get("assessments", [])
        if isinstance(claim_verifiability_context.get("assessments"), list)
        else []
    )
    links = (
        formal_public_link_context.get("links", [])
        if isinstance(formal_public_link_context.get("links"), list)
        else []
    )
    gaps = (
        representation_gap_context.get("gaps", [])
        if isinstance(representation_gap_context.get("gaps"), list)
        else []
    )
    edges = (
        diffusion_edge_context.get("edges", [])
        if isinstance(diffusion_edge_context.get("edges"), list)
        else []
    )
    agenda_counts = controversy_context_counts(
        issue_clusters=issue_clusters,
        routes=routes,
        links=links,
        gaps=gaps,
        edges=edges,
        coverages=coverages,
    )
    contract_fields = d1_contract_fields(
        board_state_source=maybe_text(current_board_state.get("state_source"))
        or "missing-board",
        coverage_source=coverage_source or "missing-coverage",
        db_path=db_path,
        deliberation_sync=deliberation_sync,
        analysis_sync=analysis_sync,
        observed_inputs={
            "board_summary_artifact_present": board_summary_file.exists(),
            "board_summary_present": isinstance(board_summary, dict),
            "board_brief_artifact_present": board_brief_file.exists(),
            "board_brief_present": bool(maybe_text(brief_text)),
            "coverage_present": bool(coverages),
            "coverage_artifact_present": bool(
                coverage_context.get("coverage_artifact_present")
            ),
            "controversy_map_present": optional_context_present(
                controversy_map_context,
                "issue_cluster_count",
            ),
            "controversy_map_artifact_present": bool(
                controversy_map_context.get("controversy_map_artifact_present")
            ),
            "verification_route_present": optional_context_present(
                verification_route_context,
                "route_count",
            ),
            "verification_route_artifact_present": bool(
                verification_route_context.get("verification_route_artifact_present")
            ),
            "claim_verifiability_present": optional_context_present(
                claim_verifiability_context,
                "assessment_count",
            ),
            "claim_verifiability_artifact_present": bool(
                claim_verifiability_context.get("claim_verifiability_artifact_present")
            ),
            "formal_public_links_present": optional_context_present(
                formal_public_link_context,
                "link_count",
            ),
            "formal_public_links_artifact_present": bool(
                formal_public_link_context.get("formal_public_links_artifact_present")
            ),
            "representation_gap_present": optional_context_present(
                representation_gap_context,
                "gap_count",
            ),
            "representation_gap_artifact_present": bool(
                representation_gap_context.get("representation_gap_artifact_present")
            ),
            "diffusion_edges_present": optional_context_present(
                diffusion_edge_context,
                "edge_count",
            ),
            "diffusion_edges_artifact_present": bool(
                diffusion_edge_context.get("diffusion_edges_artifact_present")
            ),
        },
    )
    return {
        "warnings": warnings,
        "board_state": current_board_state,
        "coverages": coverages,
        "issue_clusters": issue_clusters,
        "routes": routes,
        "assessments": assessments,
        "formal_public_links": links,
        "representation_gaps": gaps,
        "diffusion_edges": edges,
        "agenda_counts": agenda_counts,
        "board_brief_text": brief_text,
        "board_summary_file": str(board_summary_file),
        "board_brief_file": str(board_brief_file),
        "coverage_file": str(coverage_file),
        "controversy_map_file": maybe_text(controversy_map_context.get("controversy_map_file")),
        "verification_route_file": maybe_text(verification_route_context.get("verification_route_file")),
        "claim_verifiability_file": maybe_text(claim_verifiability_context.get("claim_verifiability_file")),
        "formal_public_links_file": maybe_text(formal_public_link_context.get("formal_public_links_file")),
        "representation_gap_file": maybe_text(representation_gap_context.get("representation_gap_file")),
        "diffusion_edges_file": maybe_text(diffusion_edge_context.get("diffusion_edges_file")),
        **contract_fields,
    }


def load_ranked_actions_context(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    board_summary_path: str = "",
    board_brief_path: str = "",
    coverage_path: str = "",
    max_actions: int = 6,
) -> dict[str, Any]:
    shared_context = load_d1_shared_context(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        board_summary_path=board_summary_path,
        board_brief_path=board_brief_path,
        coverage_path=coverage_path,
    )
    ranked_actions = build_actions(
        shared_context.get("board_state", {}),
        (
            shared_context.get("coverages", [])
            if isinstance(shared_context.get("coverages"), list)
            else []
        ),
        (
            shared_context.get("issue_clusters", [])
            if isinstance(shared_context.get("issue_clusters"), list)
            else []
        ),
        (
            shared_context.get("routes", [])
            if isinstance(shared_context.get("routes"), list)
            else []
        ),
        (
            shared_context.get("assessments", [])
            if isinstance(shared_context.get("assessments"), list)
            else []
        ),
        (
            shared_context.get("formal_public_links", [])
            if isinstance(shared_context.get("formal_public_links"), list)
            else []
        ),
        (
            shared_context.get("representation_gaps", [])
            if isinstance(shared_context.get("representation_gaps"), list)
            else []
        ),
        (
            shared_context.get("diffusion_edges", [])
            if isinstance(shared_context.get("diffusion_edges"), list)
            else []
        ),
        maybe_text(shared_context.get("board_brief_text")),
    )[: max(1, max_actions)]
    for action in ranked_actions:
        action.setdefault("run_id", run_id)
        action.setdefault("round_id", round_id)
    return {
        **shared_context,
        "action_source": (
            "controversy-agenda-materialization"
            if any(
                int(shared_context.get("agenda_counts", {}).get(key) or 0) > 0
                for key in (
                    "issue_cluster_count",
                    "representation_gap_count",
                    "formal_public_linkage_gap_count",
                    "diffusion_focus_count",
                )
            )
            else "coverage-ranking-fallback"
        ),
        "ranked_actions": ranked_actions,
    }
