from __future__ import annotations

from pathlib import Path
from typing import Any

from .council_objects import query_council_objects
from .phase2_proposal_actions import (
    action_from_council_proposal as shared_action_from_council_proposal,
    proposal_drives_phase2_action_queue as shared_proposal_drives_phase2_action_queue,
)
from .kernel.deliberation_plane import load_round_snapshot
from .kernel.executor import maybe_text, new_runtime_event_id, stable_hash, utc_now_iso
from .kernel.ledger import append_ledger_event, write_receipt
from .kernel.manifest import update_after_run, write_json

PLANNER_SKILL_NAME = "eco-plan-round-orchestration"
PLAN_SOURCE = "direct-council-advisory"


def resolve_run_dir(run_dir: str | Path) -> Path:
    return Path(run_dir).expanduser().resolve()


def resolve_path(run_dir: Path, override: str, default_relative: str) -> Path:
    text = maybe_text(override)
    if not text:
        return (run_dir / default_relative).resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


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


def maybe_number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def board_rollup(active_hypothesis_count: int, open_challenge_count: int, open_task_count: int, note_count: int) -> str:
    if active_hypothesis_count == 0 and open_challenge_count == 0 and open_task_count == 0 and note_count == 0:
        return "empty"
    if open_challenge_count > 0 and open_task_count == 0:
        return "needs-triage"
    if open_challenge_count > 0 or open_task_count > 0:
        return "in-flight"
    return "organized"


def snapshot_board_state(round_snapshot: dict[str, Any]) -> dict[str, Any]:
    round_state = (
        round_snapshot.get("round_state")
        if maybe_text(round_snapshot.get("status")) == "completed"
        and isinstance(round_snapshot.get("round_state"), dict)
        else {}
    )
    notes = round_state.get("notes", []) if isinstance(round_state.get("notes"), list) else []
    hypotheses = round_state.get("hypotheses", []) if isinstance(round_state.get("hypotheses"), list) else []
    challenges = round_state.get("challenge_tickets", []) if isinstance(round_state.get("challenge_tickets"), list) else []
    tasks = round_state.get("tasks", []) if isinstance(round_state.get("tasks"), list) else []
    active_hypotheses = [
        item
        for item in hypotheses
        if isinstance(item, dict) and maybe_text(item.get("status")) not in {"closed", "rejected"}
    ]
    open_challenges = [
        item
        for item in challenges
        if isinstance(item, dict) and maybe_text(item.get("status")) != "closed"
    ]
    open_tasks = [
        item
        for item in tasks
        if isinstance(item, dict) and maybe_text(item.get("status")) not in {"completed", "closed", "cancelled"}
    ]
    low_confidence_hypotheses = [
        item
        for item in active_hypotheses
        if (maybe_number(item.get("confidence")) or 0.0) < 0.6
    ]
    counts = {
        "notes_total": len(notes),
        "hypotheses_active": len(active_hypotheses),
        "hypotheses_low_confidence": len(low_confidence_hypotheses),
        "challenge_open": len(open_challenges),
        "tasks_open": len(open_tasks),
    }
    return {
        "counts": counts,
        "state_source": maybe_text(round_snapshot.get("state_source")) or "missing-board",
        "status_rollup": board_rollup(
            counts["hypotheses_active"],
            counts["challenge_open"],
            counts["tasks_open"],
            counts["notes_total"],
        ),
    }


def proposal_drives_phase2_action_queue(proposal: dict[str, Any]) -> bool:
    return shared_proposal_drives_phase2_action_queue(proposal)


def action_from_council_proposal(proposal: dict[str, Any]) -> dict[str, Any]:
    return shared_action_from_council_proposal(
        proposal,
        action_id_namespace="direct-council-action",
    )


def load_council_objects(
    run_dir: Path,
    *,
    object_kind: str,
    run_id: str,
    round_id: str,
) -> list[dict[str, Any]]:
    payload = query_council_objects(
        run_dir,
        object_kind=object_kind,
        run_id=run_id,
        round_id=round_id,
        limit=200,
    )
    return payload.get("objects", []) if isinstance(payload.get("objects"), list) else []


def load_council_proposals(run_dir: Path, *, run_id: str, round_id: str) -> list[dict[str, Any]]:
    return [
        proposal
        for proposal in load_council_objects(run_dir, object_kind="proposal", run_id=run_id, round_id=round_id)
        if isinstance(proposal, dict)
        and maybe_text(proposal.get("status")) not in {"rejected", "withdrawn", "closed"}
    ]


def load_council_readiness_opinions(run_dir: Path, *, run_id: str, round_id: str) -> list[dict[str, Any]]:
    return [
        opinion
        for opinion in load_council_objects(run_dir, object_kind="readiness-opinion", run_id=run_id, round_id=round_id)
        if isinstance(opinion, dict)
        and maybe_text(opinion.get("opinion_status")) not in {"withdrawn", "retracted"}
    ]


def load_open_probes(run_dir: Path, *, run_id: str, round_id: str) -> list[dict[str, Any]]:
    return [
        probe
        for probe in load_council_objects(run_dir, object_kind="probe", run_id=run_id, round_id=round_id)
        if isinstance(probe, dict) and maybe_text(probe.get("probe_status")) not in {"closed", "cancelled"}
    ]


def readiness_bucket(opinion: dict[str, Any]) -> str:
    readiness_value = maybe_text(opinion.get("readiness_status"))
    if bool(opinion.get("sufficient_for_promotion")) or readiness_value in {"ready", "ready-for-promotion", "promote"}:
        return "ready"
    if readiness_value in {"blocked", "reject", "rejected"}:
        return "blocked"
    return "needs-more-data"


def aggregate_council_readiness_opinions(opinions: list[dict[str, Any]]) -> dict[str, Any]:
    ready_opinions = [opinion for opinion in opinions if readiness_bucket(opinion) == "ready"]
    blocked_opinions = [opinion for opinion in opinions if readiness_bucket(opinion) == "blocked"]
    needs_more_data_opinions = [opinion for opinion in opinions if readiness_bucket(opinion) == "needs-more-data"]
    if ready_opinions and not blocked_opinions and not needs_more_data_opinions:
        readiness_status = "ready"
    elif blocked_opinions and not ready_opinions:
        readiness_status = "blocked"
    else:
        readiness_status = "needs-more-data"
    return {
        "readiness_status": readiness_status,
        "opinion_status_counts": {
            "ready": len(ready_opinions),
            "blocked": len(blocked_opinions),
            "needs-more-data": len(needs_more_data_opinions),
        },
        "basis_object_ids": unique_texts(
            [basis_id for opinion in opinions for basis_id in list_items(opinion.get("basis_object_ids"))]
        ),
        "opinion_ids": unique_texts([opinion.get("opinion_id") for opinion in opinions]),
        "evidence_refs": unique_texts(
            [evidence_ref for opinion in opinions for evidence_ref in list_items(opinion.get("evidence_refs"))]
        ),
    }


def step_entry(
    stage_name: str,
    skill_name: str,
    reason: str,
    assigned_role_hint: str,
    expected_output_path: Path,
    *,
    required_previous_stages: list[str] | None = None,
    phase_group: str = "execution",
    operator_summary: str = "",
) -> dict[str, Any]:
    return {
        "stage_name": stage_name,
        "stage_kind": "skill",
        "phase_group": phase_group,
        "skill_name": skill_name,
        "expected_skill_name": skill_name,
        "skill_args": [],
        "assigned_role_hint": assigned_role_hint,
        "required_previous_stages": required_previous_stages or [],
        "blocking": True,
        "resume_policy": "skip-if-completed",
        "operator_summary": operator_summary,
        "reason": reason,
        "expected_output_path": str(expected_output_path),
    }


def gate_step_entry(
    reason: str,
    expected_output_path: Path,
    *,
    required_previous_stages: list[str],
    readiness_stage_name: str,
) -> dict[str, Any]:
    return {
        "stage_name": "promotion-gate",
        "stage_kind": "gate",
        "phase_group": "gate",
        "required_previous_stages": required_previous_stages,
        "blocking": True,
        "resume_policy": "skip-if-completed",
        "operator_summary": "Evaluate whether the current round can move into promotion and reporting.",
        "reason": reason,
        "expected_output_path": str(expected_output_path),
        "gate_handler": "promotion-gate",
        "readiness_stage_name": readiness_stage_name,
    }


def derived_export_entry(stage_name: str, skill_name: str, reason: str, expected_output_path: Path) -> dict[str, Any]:
    return {
        "stage_name": stage_name,
        "stage_kind": "skill",
        "phase_group": "exports",
        "skill_name": skill_name,
        "expected_skill_name": skill_name,
        "assigned_role_hint": "moderator",
        "required_previous_stages": ["orchestration-planner"],
        "blocking": False,
        "resume_policy": "skip-if-completed",
        "operator_summary": reason,
        "reason": reason,
        "expected_output_path": str(expected_output_path),
        "required_for_controller": False,
        "export_mode": "derived-only",
    }


def stop_conditions(include_probe: bool) -> list[dict[str, str]]:
    rows = [
        {
            "condition_id": "planned-skill-failure",
            "trigger": "Any planned skill returns blocked or failed.",
            "effect": "Abort controller execution and surface the failing stage to runtime callers.",
        },
        {
            "condition_id": "gate-allows-promotion",
            "trigger": "Promotion gate returns allow-promote after round-readiness.",
            "effect": "Run eco-promote-evidence-basis and hand off the round to downstream reporting.",
        },
        {
            "condition_id": "gate-withholds-promotion",
            "trigger": "Promotion gate returns freeze-withheld after round-readiness.",
            "effect": "Run eco-promote-evidence-basis in withheld mode and keep investigation open.",
        },
    ]
    if include_probe:
        rows.insert(
            1,
            {
                "condition_id": "probe-stage-required",
                "trigger": "Direct council inputs or open probes still show contradiction pressure before readiness review.",
                "effect": "Keep falsification-probe materialization in the controller queue before readiness review.",
            },
        )
    return rows


def top_action_rows_from_actions(actions: list[dict[str, Any]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for action in actions[:3]:
        if not isinstance(action, dict):
            continue
        rows.append(
            {
                "action_id": maybe_text(action.get("action_id")),
                "action_kind": maybe_text(action.get("action_kind")),
                "assigned_role": maybe_text(action.get("assigned_role")),
                "priority": maybe_text(action.get("priority")),
                "objective": maybe_text(action.get("objective")),
            }
        )
    return rows


def direct_council_advisory_payload(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    output_path: str = "",
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    output_file = resolve_path(run_dir_path, output_path, f"runtime/agent_advisory_plan_{round_id}.json")
    board_summary_file = (run_dir_path / "board" / f"board_state_summary_{round_id}.json").resolve()
    board_brief_file = (run_dir_path / "board" / f"board_brief_{round_id}.md").resolve()
    probes_file = (run_dir_path / "investigation" / f"falsification_probes_{round_id}.json").resolve()
    readiness_file = (run_dir_path / "reporting" / f"round_readiness_{round_id}.json").resolve()
    promotion_gate_file = (run_dir_path / "runtime" / f"promotion_gate_{round_id}.json").resolve()
    promotion_basis_file = (run_dir_path / "promotion" / f"promoted_evidence_basis_{round_id}.json").resolve()

    round_snapshot = load_round_snapshot(
        run_dir_path,
        expected_run_id=run_id,
        round_id=round_id,
        include_closed=True,
    )
    snapshot = snapshot_board_state(round_snapshot)
    deliberation_sync = round_snapshot.get("deliberation_sync", {}) if isinstance(round_snapshot.get("deliberation_sync"), dict) else {}
    db_path = maybe_text(round_snapshot.get("db_path"))

    proposals = load_council_proposals(run_dir_path, run_id=run_id, round_id=round_id)
    proposal_actions = [action_from_council_proposal(proposal) for proposal in proposals if proposal_drives_phase2_action_queue(proposal)]
    readiness_opinions = load_council_readiness_opinions(run_dir_path, run_id=run_id, round_id=round_id)
    readiness_summary = aggregate_council_readiness_opinions(readiness_opinions) if readiness_opinions else {}
    open_probes = load_open_probes(run_dir_path, run_id=run_id, round_id=round_id)

    if not proposal_actions and not readiness_opinions:
        return {}

    readiness_status = maybe_text(readiness_summary.get("readiness_status"))
    probe_candidate_actions = [action for action in proposal_actions if bool(action.get("probe_candidate"))]
    include_probe = readiness_status != "ready" and bool(probe_candidate_actions or open_probes)

    top_action_rows = top_action_rows_from_actions(proposal_actions)
    primary_action_role = top_action_rows[0]["assigned_role"] if top_action_rows else "moderator"
    probe_reason_codes = []
    if open_probes:
        probe_reason_codes.append("open-probes")
    if probe_candidate_actions:
        probe_reason_codes.append("probe-candidate-actions")
    posture = "promote-candidate" if readiness_status == "ready" else "hold-investigation-open"
    if readiness_status:
        posture_reason_codes = [f"council-readiness-{readiness_status}"]
    else:
        posture_reason_codes = []
        if open_probes:
            posture_reason_codes.append("open-probes")
        if proposal_actions:
            posture_reason_codes.append("pending-investigation-actions")
        if include_probe:
            posture_reason_codes.append("probe-stage-retained")
        if not posture_reason_codes:
            posture_reason_codes.append("direct-council-inputs-open")

    execution_queue: list[dict[str, Any]] = []
    previous_stage_names = ["orchestration-planner"]
    if include_probe:
        execution_queue.append(
            step_entry(
                "falsification-probes",
                "eco-open-falsification-probe",
                "Open or refresh probe objects directly from council proposals or still-open DB probes.",
                "challenger",
                probes_file,
                required_previous_stages=previous_stage_names,
                operator_summary="Refresh contradiction and falsification work before readiness review.",
            )
        )
        previous_stage_names = ["falsification-probes"]
    execution_queue.append(
        step_entry(
            "round-readiness",
            "eco-summarize-round-readiness",
            "Re-evaluate round readiness directly from council readiness opinions, probe state, and governed board state.",
            "moderator",
            readiness_file,
            required_previous_stages=previous_stage_names,
            operator_summary="Freeze one readiness posture before gate evaluation.",
        )
    )
    gate_steps = [
        gate_step_entry(
            "Evaluate whether the direct-council queue leaves the round promotable or still frozen after readiness review.",
            promotion_gate_file,
            required_previous_stages=["round-readiness"],
            readiness_stage_name="round-readiness",
        )
    ]

    derived_exports = [
        derived_export_entry(
            "board-summary",
            "eco-summarize-board-state",
            "Materialize a structured board snapshot only when operators need an explicit export artifact.",
            board_summary_file,
        ),
        derived_export_entry(
            "board-brief",
            "eco-materialize-board-brief",
            "Materialize a compact human-readable board brief only when handoff or archival text is needed.",
            board_brief_file,
        ),
    ]
    post_gate_steps = [
        step_entry(
            "promotion-basis",
            "eco-promote-evidence-basis",
            "Freeze a promoted or withheld evidence basis after gate evaluation so controller output always stays auditable.",
            "moderator",
            promotion_basis_file,
            required_previous_stages=["promotion-gate"],
            phase_group="promotion",
            operator_summary="Freeze the promoted or withheld evidence basis after gate evaluation.",
        )
    ]
    fallback_path = (
        [
            {
                "when": "Promotion succeeds and the basis is frozen.",
                "reason": "The next system boundary is reporting handoff rather than more investigation work.",
                "suggested_next_skills": ["eco-materialize-reporting-handoff", "eco-draft-council-decision"],
            }
        ]
        if posture == "promote-candidate"
        else [
            {
                "when": "Gate freezes the round after readiness review.",
                "reason": top_action_rows[0]["objective"] if top_action_rows else "The board still carries unresolved council-directed investigation work.",
                "suggested_next_skills": unique_texts(
                    [
                        "eco-open-falsification-probe",
                        "eco-submit-council-proposal",
                        "eco-submit-readiness-opinion",
                    ]
                ),
            }
        ]
    )
    plan_id = "orchestration-plan-" + stable_hash("direct-council-advisory", run_id, round_id, posture, *(step["skill_name"] for step in execution_queue))[:12]
    plan_payload = {
        "schema_version": "runtime-orchestration-plan-v1",
        "skill": PLANNER_SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "plan_id": plan_id,
        "planning_status": "advisory-plan-ready",
        "planning_mode": "agent-advisory",
        "controller_authority": "advisory-only",
        "plan_source": PLAN_SOURCE,
        "probe_stage_included": include_probe,
        "downstream_posture": posture,
        "phase_decision_basis": {
            "probe_stage_reason_codes": probe_reason_codes,
            "posture_reason_codes": posture_reason_codes,
            "council_input_counts": {
                "proposal_count": len(proposals),
                "proposal_action_count": len(proposal_actions),
                "probe_candidate_action_count": len(probe_candidate_actions),
                "readiness_opinion_count": len(readiness_opinions),
                "readiness_ready_count": int(readiness_summary.get("opinion_status_counts", {}).get("ready") or 0)
                if isinstance(readiness_summary.get("opinion_status_counts"), dict)
                else 0,
                "readiness_blocked_count": int(readiness_summary.get("opinion_status_counts", {}).get("blocked") or 0)
                if isinstance(readiness_summary.get("opinion_status_counts"), dict)
                else 0,
                "readiness_needs_more_data_count": int(readiness_summary.get("opinion_status_counts", {}).get("needs-more-data") or 0)
                if isinstance(readiness_summary.get("opinion_status_counts"), dict)
                else 0,
            },
            "signal_counts": {
                "open_probe_count": len(open_probes),
                "probe_candidate_actions": len(probe_candidate_actions),
                "pending_non_promotion_actions": len(proposal_actions),
                "board_open_challenges": int(snapshot.get("counts", {}).get("challenge_open") or 0)
                if isinstance(snapshot.get("counts"), dict)
                else 0,
                "board_open_tasks": int(snapshot.get("counts", {}).get("tasks_open") or 0)
                if isinstance(snapshot.get("counts"), dict)
                else 0,
                "board_low_confidence_hypotheses": int(snapshot.get("counts", {}).get("hypotheses_low_confidence") or 0)
                if isinstance(snapshot.get("counts"), dict)
                else 0,
            },
        },
        "assigned_role_hints": unique_texts(["moderator", primary_action_role, "challenger" if include_probe else ""]),
        "agent_turn_hints": {
            "primary_role": primary_action_role or "moderator",
            "support_roles": unique_texts(["moderator", primary_action_role, "challenger" if include_probe else ""]),
            "recommended_skill_sequence": [step["skill_name"] for step in execution_queue],
        },
        "observed_state": {
            "board_present": maybe_text(snapshot.get("state_source")) == "deliberation-plane",
            "board_summary_present": False,
            "board_brief_present": False,
            "board_exports_are_derived": True,
            "direct_council_queue": True,
            "next_actions_stage_skipped": True,
            "council_proposal_count": len(proposals),
            "council_proposal_action_count": len(proposal_actions),
            "council_readiness_opinion_count": len(readiness_opinions),
            "council_readiness_status": readiness_status,
            "next_actions_present": False,
            "probes_present": bool(open_probes),
            "next_actions_source": "direct-council-advisory",
            "probes_source": "deliberation-plane-probes" if open_probes else "direct-council-advisory",
            "readiness_present": bool(readiness_opinions),
            "readiness_source": "council-readiness-opinions" if readiness_opinions else "direct-council-advisory",
            "board_state_source": maybe_text(snapshot.get("state_source")),
            "board_state_db_path": db_path,
            "status_rollup": maybe_text(snapshot.get("status_rollup")),
            "readiness_status": readiness_status,
            "counts": snapshot.get("counts", {}) if isinstance(snapshot.get("counts"), dict) else {},
            "top_actions": top_action_rows,
        },
        "inputs": {
            "board_path": str((run_dir_path / "board" / "investigation_board.json").resolve()),
            "board_summary_path": str(board_summary_file),
            "board_brief_path": str(board_brief_file),
            "next_actions_path": str((run_dir_path / "investigation" / f"next_actions_{round_id}.json").resolve()),
            "probes_path": str(probes_file),
            "readiness_path": str(readiness_file),
        },
        "execution_queue": execution_queue,
        "gate_steps": gate_steps,
        "derived_exports": derived_exports,
        "post_gate_steps": post_gate_steps,
        "stop_conditions": stop_conditions(include_probe),
        "fallback_path": fallback_path,
        "planning_notes": [
            "Direct council advisory compiler materialized this plan without a planner skill subprocess.",
            "The advisory queue is compiled from DB-backed council proposals, readiness opinions, and open probes.",
            "Board summary and board brief remain derived exports rather than controller prerequisites.",
        ],
        "deliberation_sync": deliberation_sync,
    }
    write_json(output_file, plan_payload)

    artifact_refs = [
        {
            "signal_id": "",
            "artifact_path": str(output_file),
            "record_locator": "$",
            "artifact_ref": f"{output_file}:$",
        }
    ]
    return {
        "status": "completed",
        "summary": {
            "skill": PLANNER_SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "plan_id": plan_id,
            "plan_source": PLAN_SOURCE,
            "planned_skill_count": len(execution_queue) + len(post_gate_steps),
            "gate_step_count": len(gate_steps),
            "planned_stage_count": len(execution_queue) + len(gate_steps) + len(post_gate_steps),
            "derived_export_count": len(derived_exports),
            "planning_mode": "agent-advisory",
            "direct_council_queue": True,
            "probe_stage_included": include_probe,
            "downstream_posture": posture,
            "board_state_source": maybe_text(snapshot.get("state_source")),
            "db_path": db_path,
        },
        "plan_source": PLAN_SOURCE,
        "receipt_id": "runtime-receipt-" + stable_hash(PLANNER_SKILL_NAME, run_id, round_id, plan_id, "direct-council")[:20],
        "batch_id": "runtimebatch-" + stable_hash(PLANNER_SKILL_NAME, run_id, round_id, output_file.name, "direct-council")[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [plan_id],
        "warnings": [],
        "deliberation_sync": deliberation_sync,
        "board_handoff": {
            "candidate_ids": [plan_id, *list_items(readiness_summary.get("basis_object_ids"))],
            "evidence_refs": artifact_refs,
            "gap_hints": [] if posture == "promote-candidate" else ["Current council state still points to investigation hold rather than clean promotion."],
            "challenge_hints": [f"{len(open_probes)} open probes remain visible to the direct advisory compiler."] if open_probes else [],
            "suggested_next_skills": unique_texts(
                [step["skill_name"] for step in execution_queue]
                + [step["skill_name"] for step in post_gate_steps]
                + [skill_name for row in fallback_path for skill_name in row.get("suggested_next_skills", [])]
            ),
        },
    }


def materialize_direct_council_advisory_plan(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    output_path: str = "",
    contract_mode: str = "warn",
) -> dict[str, Any]:
    started_at = utc_now_iso()
    payload = direct_council_advisory_payload(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        output_path=output_path,
    )
    if not payload:
        return {}
    resolved_run_dir = resolve_run_dir(run_dir)
    finished_at = utc_now_iso()
    event_id = new_runtime_event_id(
        "runtimeevt",
        run_id,
        round_id,
        "direct-council-advisory-plan",
        maybe_text(payload.get("summary", {}).get("plan_id")) if isinstance(payload.get("summary"), dict) else "",
        started_at,
        finished_at,
    )
    receipt_id = maybe_text(payload.get("receipt_id")) or ("runtime-receipt-" + stable_hash(run_id, round_id, event_id)[:20])
    receipt_file = write_receipt(resolved_run_dir, receipt_id, payload)
    event = {
        "schema_version": "runtime-event-v3",
        "event_id": event_id,
        "event_type": "direct-council-advisory-plan",
        "run_id": run_id,
        "round_id": round_id,
        "started_at_utc": started_at,
        "completed_at_utc": finished_at,
        "status": "completed",
        "contract_mode": contract_mode,
        "skill_name": PLANNER_SKILL_NAME,
        "receipt_id": receipt_id,
        "batch_id": maybe_text(payload.get("batch_id")),
        "artifact_refs": payload.get("artifact_refs", []) if isinstance(payload.get("artifact_refs"), list) else [],
        "canonical_ids": payload.get("canonical_ids", []) if isinstance(payload.get("canonical_ids"), list) else [],
        "summary": payload.get("summary", {}) if isinstance(payload.get("summary"), dict) else {},
        "receipt_path": str(receipt_file),
    }
    append_ledger_event(resolved_run_dir, event)
    manifest, cursor = update_after_run(
        resolved_run_dir,
        run_id=run_id,
        round_id=round_id,
        skill_name=PLANNER_SKILL_NAME,
        receipt_id=receipt_id,
        event_id=event_id,
    )
    return {
        "status": "completed",
        "summary": {
            "skill_name": PLANNER_SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "event_id": event_id,
            "receipt_id": receipt_id,
            "contract_mode": contract_mode,
            "attempt_count": 1,
            "recovered_after_retry": False,
            "timeout_seconds": None,
            "retry_budget": 0,
        },
        "event": event,
        "manifest": manifest,
        "cursor": cursor,
        "skill_payload": payload,
    }


__all__ = ["direct_council_advisory_payload", "materialize_direct_council_advisory_plan"]
