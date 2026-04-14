from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from .analysis_plane import load_evidence_coverage_context
from .deliberation_plane import (
    load_falsification_probe_snapshot,
    load_moderator_action_snapshot,
    load_round_snapshot,
)

PRIORITY_WEIGHT = {"critical": 4.0, "high": 3.0, "medium": 2.0, "low": 1.0}
ACTION_KIND_WEIGHT = {
    "resolve-challenge": 2.6,
    "resolve-contradiction": 2.4,
    "finish-board-task": 2.1,
    "classify-verifiability": 1.95,
    "stabilize-hypothesis": 1.8,
    "expand-coverage": 1.7,
    "prepare-promotion": 1.2,
}
EXPLICIT_D1_INPUT_KEYS = {
    "board_summary_artifact_present",
    "board_summary_present",
    "board_brief_artifact_present",
    "board_brief_present",
    "coverage_artifact_present",
    "coverage_present",
    "next_actions_artifact_present",
    "next_actions_present",
    "probes_artifact_present",
    "probes_present",
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


def load_next_actions_wrapper(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    next_actions_path: str = "",
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    next_actions_file = resolve_path(
        run_dir_path,
        next_actions_path,
        f"investigation/next_actions_{round_id}.json",
    )
    artifact_payload = load_json_if_exists(next_actions_file)
    if isinstance(artifact_payload, dict):
        return {
            "payload": artifact_payload,
            "source": maybe_text(artifact_payload.get("action_source"))
            or "next-actions-artifact",
            "artifact_path": str(next_actions_file),
            "artifact_present": True,
            "payload_present": True,
        }
    snapshot_payload = load_moderator_action_snapshot(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    if isinstance(snapshot_payload, dict):
        payload = dict(snapshot_payload)
        payload["action_source"] = (
            maybe_text(payload.get("action_source"))
            or "deliberation-plane-actions"
        )
        return {
            "payload": payload,
            "source": "deliberation-plane-actions",
            "artifact_path": str(next_actions_file),
            "artifact_present": False,
            "payload_present": True,
        }
    return {
        "payload": None,
        "source": "missing-next-actions",
        "artifact_path": str(next_actions_file),
        "artifact_present": False,
        "payload_present": False,
    }


def load_falsification_probe_wrapper(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    probes_path: str = "",
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    probes_file = resolve_path(
        run_dir_path,
        probes_path,
        f"investigation/falsification_probes_{round_id}.json",
    )
    artifact_payload = load_json_if_exists(probes_file)
    if isinstance(artifact_payload, dict):
        return {
            "payload": artifact_payload,
            "source": maybe_text(artifact_payload.get("action_source"))
            or "falsification-probes-artifact",
            "artifact_path": str(probes_file),
            "artifact_present": True,
            "payload_present": True,
        }
    snapshot_payload = load_falsification_probe_snapshot(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    if isinstance(snapshot_payload, dict):
        payload = dict(snapshot_payload)
        payload["action_source"] = (
            maybe_text(payload.get("action_source"))
            or "deliberation-plane-probes"
        )
        return {
            "payload": payload,
            "source": "deliberation-plane-probes",
            "artifact_path": str(probes_file),
            "artifact_present": False,
            "payload_present": True,
        }
    return {
        "payload": None,
        "source": "missing-probes",
        "artifact_path": str(probes_file),
        "artifact_present": False,
        "payload_present": False,
    }


def excerpt_text(text: str, limit: int = 180) -> str:
    normalized = maybe_text(text)
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3].rstrip() + "..."


def priority_score(priority: str) -> float:
    return PRIORITY_WEIGHT.get(maybe_text(priority).lower(), PRIORITY_WEIGHT["medium"])


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
    normalized["board_summary_artifact_present"] = bool(
        source.get(
            "board_summary_artifact_present",
            source.get("board_summary_present", False),
        )
    )
    normalized["board_summary_present"] = bool(
        source.get("board_summary_present", False)
    )
    normalized["board_brief_artifact_present"] = bool(
        source.get(
            "board_brief_artifact_present",
            source.get("board_brief_present", False),
        )
    )
    normalized["board_brief_present"] = bool(
        source.get("board_brief_present", False)
    )
    normalized["coverage_artifact_present"] = bool(
        source.get("coverage_artifact_present", False)
    )
    normalized["coverage_present"] = bool(source.get("coverage_present", False))
    normalized["next_actions_artifact_present"] = bool(
        source.get(
            "next_actions_artifact_present",
            source.get("next_actions_present", False),
        )
    )
    normalized["next_actions_present"] = bool(
        source.get("next_actions_present", False)
    )
    normalized["probes_artifact_present"] = bool(
        source.get("probes_artifact_present", source.get("probes_present", False))
    )
    normalized["probes_present"] = bool(source.get("probes_present", False))
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
    action_kind_component = ACTION_KIND_WEIGHT.get(
        maybe_text(payload.get("action_kind")),
        1.0,
    )
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
    probe_component = 0.5 if bool(payload.get("probe_candidate")) else 0.0
    return round(
        priority_component
        + action_kind_component
        + contradiction_component
        + coverage_component
        + uncertainty_component
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


def action_from_open_challenge(challenge: dict[str, Any], brief_context: str) -> dict[str, Any]:
    ticket_id = maybe_text(challenge.get("ticket_id"))
    target_claim_id = maybe_text(challenge.get("target_claim_id"))
    target_hypothesis_id = maybe_text(challenge.get("target_hypothesis_id"))
    return {
        "action_id": "action-" + stable_hash("d1-action", ticket_id, "challenge")[:12],
        "action_kind": "resolve-challenge",
        "priority": maybe_text(challenge.get("priority")) or "high",
        "assigned_role": maybe_text(challenge.get("owner_role")) or "challenger",
        "objective": maybe_text(challenge.get("title"))
        or "Resolve an open controversy point before closing the round.",
        "reason": maybe_text(challenge.get("title"))
        or "An open contested point still needs follow-up before the controversy map is stable.",
        "source_ids": unique_texts([ticket_id, target_claim_id, target_hypothesis_id]),
        "target": {
            "ticket_id": ticket_id,
            "claim_id": target_claim_id,
            "hypothesis_id": target_hypothesis_id,
        },
        "controversy_gap": "unresolved-contestation",
        "recommended_lane": "probe-before-closure",
        "expected_outcome": "Decide whether the contested point needs verification, rebuttal, or reframing.",
        "evidence_refs": unique_texts(
            challenge.get("linked_artifact_refs", [])
            if isinstance(challenge.get("linked_artifact_refs"), list)
            else []
        ),
        "probe_candidate": True,
        "contradiction_link_count": 1,
        "coverage_score": 0.45,
        "confidence": None,
        "brief_context": brief_context,
    }


def action_from_open_task(task: dict[str, Any], brief_context: str) -> dict[str, Any]:
    task_id = maybe_text(task.get("task_id"))
    return {
        "action_id": "action-" + stable_hash("d1-action", task_id, "task")[:12],
        "action_kind": "finish-board-task",
        "priority": maybe_text(task.get("priority")) or "medium",
        "assigned_role": maybe_text(task.get("owner_role")) or "moderator",
        "objective": maybe_text(task.get("title"))
        or "Finish a board task that blocks controversy-map completion.",
        "reason": maybe_text(task.get("title"))
        or maybe_text(task.get("task_text"))
        or "A board coordination task is still in flight.",
        "source_ids": unique_texts(
            [task_id, task.get("source_ticket_id"), task.get("source_hypothesis_id")]
        ),
        "target": {
            "task_id": task_id,
            "ticket_id": maybe_text(task.get("source_ticket_id")),
            "hypothesis_id": maybe_text(task.get("source_hypothesis_id")),
        },
        "controversy_gap": "board-coordination-gap",
        "recommended_lane": "board-followthrough",
        "expected_outcome": "Finish the coordination work needed to advance the round.",
        "evidence_refs": unique_texts(
            task.get("linked_artifact_refs", [])
            if isinstance(task.get("linked_artifact_refs"), list)
            else []
        ),
        "probe_candidate": False,
        "contradiction_link_count": 0,
        "coverage_score": 0.55,
        "confidence": None,
        "brief_context": brief_context,
    }


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
    return {
        "action_id": "action-"
        + stable_hash("d1-action", hypothesis_id, "hypothesis")[:12],
        "action_kind": "stabilize-hypothesis",
        "priority": "high" if (confidence or 0.0) < 0.6 else "medium",
        "assigned_role": maybe_text(hypothesis.get("owner_role")) or "moderator",
        "objective": maybe_text(hypothesis.get("title"))
        or "Stabilize an active issue interpretation.",
        "reason": "The board still carries an active hypothesis with limited confidence.",
        "source_ids": unique_texts([hypothesis_id] + linked_claim_ids),
        "target": {
            "hypothesis_id": hypothesis_id,
            "claim_id": linked_claim_ids[0] if linked_claim_ids else "",
        },
        "controversy_gap": "issue-structure-gap",
        "recommended_lane": "clarify-board-position",
        "expected_outcome": "Clarify whether the active interpretation should stay open, split, or be retired.",
        "evidence_refs": [],
        "probe_candidate": (confidence or 0.0) < 0.6,
        "contradiction_link_count": 0,
        "coverage_score": 0.5,
        "confidence": confidence,
        "brief_context": brief_context,
    }


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
    return {
        "action_id": "action-"
        + stable_hash("d1-action", coverage_id, action_kind)[:12],
        "action_kind": action_kind,
        "priority": priority,
        "assigned_role": assigned_role,
        "objective": objective,
        "reason": reason,
        "source_ids": unique_texts([coverage_id, claim_id]),
        "target": {"coverage_id": coverage_id, "claim_id": claim_id},
        "controversy_gap": controversy_gap,
        "recommended_lane": recommended_lane,
        "expected_outcome": expected_outcome,
        "evidence_refs": unique_texts(
            coverage.get("evidence_refs", [])
            if isinstance(coverage.get("evidence_refs"), list)
            else []
        ),
        "probe_candidate": contradiction_count > 0
        or readiness == "weak"
        or action_kind == "classify-verifiability",
        "contradiction_link_count": contradiction_count,
        "coverage_score": float(coverage.get("coverage_score") or 0.0),
        "confidence": None,
        "brief_context": brief_context,
    }


def prepare_promotion_action(
    coverage: dict[str, Any],
    brief_context: str,
) -> dict[str, Any]:
    coverage_id = maybe_text(coverage.get("coverage_id"))
    claim_id = maybe_text(coverage.get("claim_id"))
    return {
        "action_id": "action-"
        + stable_hash("d1-action", coverage_id, "promotion")[:12],
        "action_kind": "prepare-promotion",
        "priority": "medium",
        "assigned_role": "moderator",
        "objective": "Prepare the round for readiness review once the controversy map is stable.",
        "reason": f"Claim {claim_id} already has strong evidence coverage and can move toward readiness gating.",
        "source_ids": unique_texts([coverage_id, claim_id]),
        "target": {"coverage_id": coverage_id, "claim_id": claim_id},
        "controversy_gap": "promotion-readiness",
        "recommended_lane": "promotion-review",
        "expected_outcome": "Freeze the strongest support path for board and reporting review.",
        "evidence_refs": unique_texts(
            coverage.get("evidence_refs", [])
            if isinstance(coverage.get("evidence_refs"), list)
            else []
        ),
        "probe_candidate": False,
        "contradiction_link_count": 0,
        "coverage_score": float(coverage.get("coverage_score") or 0.0),
        "confidence": None,
        "brief_context": brief_context,
    }


def build_actions(
    board_state: dict[str, Any],
    coverages: list[dict[str, Any]],
    brief_text: str,
) -> list[dict[str, Any]]:
    brief_context = excerpt_text(brief_text)
    actions: list[dict[str, Any]] = []
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
    analysis_sync = (
        coverage_context.get("analysis_sync")
        if isinstance(coverage_context.get("analysis_sync"), dict)
        else {}
    )
    if not db_path:
        db_path = maybe_text(coverage_context.get("db_path"))
    brief_text = load_text_if_exists(board_brief_file)
    current_board_state = board_snapshot(
        round_state,
        board_summary if isinstance(board_summary, dict) else None,
        include_notes=include_board_notes,
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
        },
    )
    return {
        "warnings": warnings,
        "board_state": current_board_state,
        "coverages": coverages,
        "board_brief_text": brief_text,
        "board_summary_file": str(board_summary_file),
        "board_brief_file": str(board_brief_file),
        "coverage_file": str(coverage_file),
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
        maybe_text(shared_context.get("board_brief_text")),
    )[: max(1, max_actions)]
    for action in ranked_actions:
        action.setdefault("run_id", run_id)
        action.setdefault("round_id", round_id)
    return {
        **shared_context,
        "ranked_actions": ranked_actions,
    }
