from __future__ import annotations

import shlex
from pathlib import Path
from typing import Any

from .analysis_plane import query_analysis_result_sets
from .deliberation_plane import load_round_snapshot
from .executor import (
    SkillExecutionError,
    maybe_text,
    new_runtime_event_id,
    run_skill,
    skill_command_hint,
    utc_now_iso,
)
from .ledger import append_ledger_event
from .manifest import load_json_if_exists, write_json
from .operations import load_admission_policy, runtime_health_payload
from .paths import (
    agent_advisory_plan_path,
    agent_entry_gate_path,
    mission_scaffold_path,
    resolve_run_dir,
)
from .supervisor import suggest_next_round_id


ROLE_DEFINITIONS = [
    {
        "role": "sociologist",
        "focus": "public evidence query, narrative regrouping, and claim-side analysis.",
        "read_skills": ["eco-read-board-delta", "eco-query-public-signals"],
        "write_skills": ["eco-post-board-note", "eco-update-hypothesis-status"],
        "analysis_kinds": ["claim-candidate", "claim-cluster", "claim-scope", "evidence-coverage"],
    },
    {
        "role": "environmentalist",
        "focus": "environment evidence query, observation analysis, and corroboration work.",
        "read_skills": ["eco-read-board-delta", "eco-query-environment-signals"],
        "write_skills": ["eco-post-board-note", "eco-update-hypothesis-status"],
        "analysis_kinds": ["observation-candidate", "merged-observation", "observation-scope", "evidence-coverage"],
    },
    {
        "role": "challenger",
        "focus": "contradiction pressure, challenge tickets, and falsification probes.",
        "read_skills": ["eco-read-board-delta", "eco-query-public-signals", "eco-query-environment-signals"],
        "write_skills": ["eco-open-challenge-ticket", "eco-open-falsification-probe", "eco-close-challenge-ticket"],
        "analysis_kinds": ["claim-cluster", "merged-observation", "evidence-coverage"],
    },
    {
        "role": "moderator",
        "focus": "board state progression, round transition, and return to runtime hard gates.",
        "read_skills": ["eco-read-board-delta"],
        "write_skills": ["eco-post-board-note", "eco-claim-board-task", "eco-open-investigation-round"],
        "analysis_kinds": ["claim-cluster", "merged-observation", "evidence-coverage"],
    },
]


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


def kernel_command(command_name: str, *args: str) -> str:
    return shlex.join(
        [
            "python3",
            "eco-concil-runtime/scripts/eco_runtime_kernel.py",
            command_name,
            *args,
        ]
    )


def run_skill_command(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    skill_name: str,
    contract_mode: str,
    skill_args: list[str] | None = None,
) -> str:
    return (
        "python3 eco-concil-runtime/scripts/eco_runtime_kernel.py "
        + skill_command_hint(
            "run-skill",
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name=skill_name,
            contract_mode=contract_mode,
            skill_args=skill_args or [],
        )
    )


def query_result_set_command(*, run_dir: Path, run_id: str, round_id: str, analysis_kind: str) -> str:
    return kernel_command(
        "list-analysis-result-sets",
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--analysis-kind",
        analysis_kind,
        "--latest-only",
        "--include-contract",
        "--pretty",
    )


def query_result_item_template(*, run_dir: Path, run_id: str, round_id: str, analysis_kind: str) -> str:
    return kernel_command(
        "query-analysis-result-items",
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--analysis-kind",
        analysis_kind,
        "--latest-only",
        "--subject-id",
        f"<{analysis_kind.replace('-', '_')}_id>",
        "--include-result-sets",
        "--include-contract",
        "--pretty",
    )


def advisory_plan_relative_path(run_dir: Path, round_id: str) -> str:
    return str(agent_advisory_plan_path(run_dir, round_id).relative_to(run_dir))


def advisory_plan_command(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    contract_mode: str,
) -> str:
    return run_skill_command(
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        skill_name="eco-plan-round-orchestration",
        contract_mode=contract_mode,
        skill_args=[
            "--planner-mode",
            "agent-advisory",
            "--output-path",
            advisory_plan_relative_path(run_dir, round_id),
        ],
    )


def board_counts(round_state: dict[str, Any]) -> dict[str, int]:
    notes = round_state.get("notes", []) if isinstance(round_state.get("notes"), list) else []
    hypotheses = round_state.get("hypotheses", []) if isinstance(round_state.get("hypotheses"), list) else []
    challenges = round_state.get("challenge_tickets", []) if isinstance(round_state.get("challenge_tickets"), list) else []
    tasks = round_state.get("tasks", []) if isinstance(round_state.get("tasks"), list) else []
    return {
        "note_count": len(notes),
        "hypothesis_count": len(hypotheses),
        "active_hypothesis_count": len(
            [item for item in hypotheses if isinstance(item, dict) and maybe_text(item.get("status")) not in {"closed", "rejected"}]
        ),
        "challenge_ticket_count": len(challenges),
        "open_challenge_count": len(
            [item for item in challenges if isinstance(item, dict) and maybe_text(item.get("status")) != "closed"]
        ),
        "task_count": len(tasks),
        "open_task_count": len(
            [
                item
                for item in tasks
                if isinstance(item, dict)
                and maybe_text(item.get("status")) not in {"completed", "closed", "cancelled"}
            ]
        ),
    }


def round_surface(run_dir: Path, *, run_id: str, round_id: str) -> dict[str, Any]:
    snapshot = load_round_snapshot(
        run_dir,
        expected_run_id=run_id,
        round_id=round_id,
        include_closed=True,
    )
    round_state = snapshot.get("round_state", {}) if isinstance(snapshot.get("round_state"), dict) else {}
    counts = board_counts(round_state) if maybe_text(snapshot.get("status")) == "completed" else {
        "note_count": 0,
        "hypothesis_count": 0,
        "active_hypothesis_count": 0,
        "challenge_ticket_count": 0,
        "open_challenge_count": 0,
        "task_count": 0,
        "open_task_count": 0,
    }
    return {
        "status": maybe_text(snapshot.get("status")) or "missing-board",
        "state_source": maybe_text(snapshot.get("state_source")) or "missing-board",
        "board_path": maybe_text(snapshot.get("board_path")),
        "db_path": maybe_text(snapshot.get("db_path")),
        "counts": counts,
        "deliberation_sync": snapshot.get("deliberation_sync", {}) if isinstance(snapshot.get("deliberation_sync"), dict) else {},
    }


def analysis_surface(run_dir: Path, *, run_id: str, round_id: str) -> dict[str, Any]:
    try:
        payload = query_analysis_result_sets(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            latest_only=True,
            limit=200,
            offset=0,
        )
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "failed",
            "db_path": "",
            "matching_result_set_count": 0,
            "analysis_kind_count": 0,
            "available_analysis_kinds": [],
            "warnings": [
                {
                    "code": "analysis-query-failed",
                    "message": str(exc),
                }
            ],
        }
    rows = payload.get("result_sets", []) if isinstance(payload.get("result_sets"), list) else []
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        analysis_kind = maybe_text(row.get("analysis_kind"))
        if not analysis_kind:
            continue
        summary = grouped.setdefault(
            analysis_kind,
            {
                "analysis_kind": analysis_kind,
                "result_set_count": 0,
                "item_count": 0,
                "artifact_missing_count": 0,
                "latest_generated_at_utc": "",
            },
        )
        summary["result_set_count"] += 1
        summary["item_count"] += int(row.get("item_count") or 0)
        if not bool(row.get("artifact_present")):
            summary["artifact_missing_count"] += 1
        generated_at = maybe_text(row.get("generated_at_utc"))
        if generated_at and generated_at > maybe_text(summary.get("latest_generated_at_utc")):
            summary["latest_generated_at_utc"] = generated_at
    return {
        "status": "completed",
        "db_path": maybe_text(payload.get("summary", {}).get("db_path"))
        if isinstance(payload.get("summary"), dict)
        else "",
        "matching_result_set_count": int(
            payload.get("summary", {}).get("matching_result_set_count") or 0
        )
        if isinstance(payload.get("summary"), dict)
        else 0,
        "analysis_kind_count": len(grouped),
        "available_analysis_kinds": sorted(
            grouped.values(),
            key=lambda item: (
                -int(item.get("result_set_count") or 0),
                maybe_text(item.get("analysis_kind")),
            ),
        ),
        "warnings": payload.get("warnings", []) if isinstance(payload.get("warnings"), list) else [],
    }


def governance_surface(run_dir: Path, *, round_id: str) -> dict[str, Any]:
    policy = load_admission_policy(run_dir)
    health = runtime_health_payload(run_dir, round_id=round_id)
    return {
        "permission_profile": maybe_text(policy.get("permission_profile")) or "standard",
        "approval_authority": maybe_text(policy.get("approval_authority")) or "runtime-operator",
        "rollback_mode": maybe_text(policy.get("rollback_policy", {}).get("mode"))
        if isinstance(policy.get("rollback_policy"), dict)
        else "operator-mediated",
        "alert_status": maybe_text(health.get("alert_status")) or "green",
        "open_dead_letter_count": int(health.get("summary", {}).get("open_dead_letter_count") or 0)
        if isinstance(health.get("summary"), dict)
        else 0,
        "admission_policy_path": maybe_text(policy.get("policy_path")) or "",
        "runtime_health_path": maybe_text(health.get("output_path")) or "",
    }


def mission_surface(run_dir: Path, round_id: str) -> dict[str, Any]:
    payload = load_json_if_exists(mission_scaffold_path(run_dir, round_id)) or {}
    return {
        "present": bool(payload),
        "path": str(mission_scaffold_path(run_dir, round_id).resolve()),
        "orchestration_mode": maybe_text(payload.get("orchestration_mode")),
        "scaffold_id": maybe_text(payload.get("scaffold_id")),
        "task_count": int(payload.get("task_count") or 0),
        "import_source_count": int(payload.get("import_source_count") or 0),
        "request_source_count": int(payload.get("request_source_count") or 0),
    }


def advisory_plan_surface(run_dir: Path, round_id: str) -> dict[str, Any]:
    payload = load_json_if_exists(agent_advisory_plan_path(run_dir, round_id)) or {}
    return {
        "present": bool(payload),
        "path": str(agent_advisory_plan_path(run_dir, round_id).resolve()),
        "planning_mode": maybe_text(payload.get("planning_mode")),
        "controller_authority": maybe_text(payload.get("controller_authority")),
        "downstream_posture": maybe_text(payload.get("downstream_posture")),
        "recommended_skill_sequence": payload.get("agent_turn_hints", {}).get("recommended_skill_sequence", [])
        if isinstance(payload.get("agent_turn_hints"), dict)
        and isinstance(payload.get("agent_turn_hints", {}).get("recommended_skill_sequence"), list)
        else [],
        "primary_role": maybe_text(payload.get("agent_turn_hints", {}).get("primary_role"))
        if isinstance(payload.get("agent_turn_hints"), dict)
        else "",
        "support_roles": payload.get("agent_turn_hints", {}).get("support_roles", [])
        if isinstance(payload.get("agent_turn_hints"), dict)
        and isinstance(payload.get("agent_turn_hints", {}).get("support_roles"), list)
        else [],
    }


def entry_status(
    *,
    governance: dict[str, Any],
    mission: dict[str, Any],
    round_surface_payload: dict[str, Any],
    analysis: dict[str, Any],
) -> tuple[str, list[dict[str, str]]]:
    warnings: list[dict[str, str]] = []
    if (
        not mission.get("present")
        and maybe_text(round_surface_payload.get("state_source")) == "missing-board"
        and int(analysis.get("matching_result_set_count") or 0) == 0
    ):
        warnings.append(
            {
                "code": "missing-entry-state",
                "message": "No mission scaffold, board state, or analysis result sets are available for the selected round.",
            }
        )
        return "blocked", warnings
    if maybe_text(governance.get("alert_status")) == "red" or int(governance.get("open_dead_letter_count") or 0) > 0:
        warnings.append(
            {
                "code": "operator-review-required",
                "message": "Runtime health is not clean; inspect dead letters and health alerts before trusting agent-side conclusions.",
            }
        )
        return "needs-operator-review", warnings
    if maybe_text(round_surface_payload.get("state_source")) == "missing-board":
        warnings.append(
            {
                "code": "missing-board-snapshot",
                "message": "Board state has not been initialized yet; the entry gate will rely on mission and analysis surfaces until board state exists.",
            }
        )
    if int(analysis.get("matching_result_set_count") or 0) == 0:
        warnings.append(
            {
                "code": "analysis-surface-empty",
                "message": "No analysis-plane result sets are visible yet; direct signal-plane query skills remain the primary agent entry reads.",
            }
        )
    return "ready", warnings


def role_entry_points(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    contract_mode: str,
    next_round_id: str,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for definition in ROLE_DEFINITIONS:
        role = maybe_text(definition.get("role"))
        role_read_commands: list[str] = []
        for skill_name in definition.get("read_skills", []) if isinstance(definition.get("read_skills"), list) else []:
            if skill_name == "eco-read-board-delta":
                role_read_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        contract_mode=contract_mode,
                        skill_args=["--include-closed", "--event-limit", "20"],
                    )
                )
            else:
                role_read_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        contract_mode=contract_mode,
                    )
                )
        analysis_commands = [
            query_result_set_command(
                run_dir=run_dir,
                run_id=run_id,
                round_id=round_id,
                analysis_kind=analysis_kind,
            )
            for analysis_kind in definition.get("analysis_kinds", []) if isinstance(definition.get("analysis_kinds"), list)
        ]
        role_write_commands: list[str] = []
        for skill_name in definition.get("write_skills", []) if isinstance(definition.get("write_skills"), list) else []:
            if skill_name == "eco-post-board-note":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        contract_mode=contract_mode,
                        skill_args=[
                            "--author-role",
                            role,
                            "--category",
                            "analysis",
                            "--note-text",
                            "<note_text>",
                        ],
                    )
                )
            elif skill_name == "eco-update-hypothesis-status":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        contract_mode=contract_mode,
                        skill_args=[
                            "--title",
                            "<hypothesis_title>",
                            "--statement",
                            "<hypothesis_statement>",
                            "--status",
                            "active",
                            "--owner-role",
                            role if role in {"sociologist", "environmentalist", "moderator"} else "moderator",
                        ],
                    )
                )
            elif skill_name == "eco-open-challenge-ticket":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        contract_mode=contract_mode,
                        skill_args=[
                            "--title",
                            "<challenge_title>",
                            "--challenge-statement",
                            "<challenge_statement>",
                            "--priority",
                            "high",
                            "--owner-role",
                            "challenger",
                        ],
                    )
                )
            elif skill_name == "eco-open-falsification-probe":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        contract_mode=contract_mode,
                        skill_args=["--max-probes", "3"],
                    )
                )
            elif skill_name == "eco-close-challenge-ticket":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        contract_mode=contract_mode,
                        skill_args=["--ticket-id", "<ticket_id>"],
                    )
                )
            elif skill_name == "eco-claim-board-task":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        contract_mode=contract_mode,
                        skill_args=["--task-id", "<task_id>", "--claimed-by-role", "moderator"],
                    )
                )
            elif skill_name == "eco-open-investigation-round":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=next_round_id,
                        skill_name=skill_name,
                        contract_mode=contract_mode,
                        skill_args=["--source-round-id", round_id],
                    )
                )
        results.append(
            {
                "role": role,
                "focus": maybe_text(definition.get("focus")),
                "read_commands": role_read_commands,
                "analysis_commands": analysis_commands,
                "write_commands": role_write_commands,
            }
        )
    return results


def hard_gate_commands(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    next_round_id: str,
    contract_mode: str,
) -> dict[str, str]:
    return {
        "show_run_state": kernel_command(
            "show-run-state",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--tail",
            "20",
            "--pretty",
        ),
        "supervise_round": kernel_command(
            "supervise-round",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--pretty",
        ),
        "apply_promotion_gate": kernel_command(
            "apply-promotion-gate",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--pretty",
        ),
        "close_round": kernel_command(
            "close-round",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--pretty",
        ),
        "open_next_round": run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=next_round_id,
            skill_name="eco-open-investigation-round",
            contract_mode=contract_mode,
            skill_args=["--source-round-id", round_id],
        ),
    }


def entry_chain(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    contract_mode: str,
    next_round_id: str,
) -> list[dict[str, str]]:
    return [
        {
            "step_id": "inspect-runtime",
            "mode": "runtime-gate",
            "objective": "Inspect governance, health, and current round posture before agent work begins.",
            "command": kernel_command(
                "show-run-state",
                "--run-dir",
                str(run_dir),
                "--round-id",
                round_id,
                "--tail",
                "20",
                "--pretty",
            ),
        },
        {
            "step_id": "read-board-delta",
            "mode": "advisory-read",
            "objective": "Read the current round directly from deliberation-plane state instead of depending on stale summaries.",
            "command": run_skill_command(
                run_dir=run_dir,
                run_id=run_id,
                round_id=round_id,
                skill_name="eco-read-board-delta",
                contract_mode=contract_mode,
                skill_args=["--include-closed", "--event-limit", "20"],
            ),
        },
        {
            "step_id": "query-shared-planes",
            "mode": "advisory-read",
            "objective": "Read normalized public/environment signals and current analysis result sets without leaving governed runtime surfaces.",
            "command": run_skill_command(
                run_dir=run_dir,
                run_id=run_id,
                round_id=round_id,
                skill_name="eco-query-public-signals",
                contract_mode=contract_mode,
            ),
        },
        {
            "step_id": "materialize-agent-advisory-plan",
            "mode": "advisory-plan",
            "objective": "Refresh one advisory-only orchestration plan so the agent route has an explicit but non-binding next-step hint.",
            "command": advisory_plan_command(
                run_dir=run_dir,
                run_id=run_id,
                round_id=round_id,
                contract_mode=contract_mode,
            ),
        },
        {
            "step_id": "write-deliberation-state",
            "mode": "governed-write",
            "objective": "Push findings back through explicit board or challenge write skills instead of hidden in-memory agent state.",
            "command": run_skill_command(
                run_dir=run_dir,
                run_id=run_id,
                round_id=round_id,
                skill_name="eco-post-board-note",
                contract_mode=contract_mode,
                skill_args=[
                    "--author-role",
                    "moderator",
                    "--category",
                    "analysis",
                    "--note-text",
                    "<note_text>",
                ],
            ),
        },
        {
            "step_id": "return-to-runtime-gate",
            "mode": "runtime-gate",
            "objective": "Hand the updated round back to the runtime supervisor or promotion gate for a governed decision.",
            "command": kernel_command(
                "supervise-round",
                "--run-dir",
                str(run_dir),
                "--run-id",
                run_id,
                "--round-id",
                round_id,
                "--pretty",
            ),
        },
        {
            "step_id": "open-follow-up-round",
            "mode": "governed-write",
            "objective": "If the round remains open, advance the investigation with an explicit next-round transition rather than implicit carryover.",
            "command": run_skill_command(
                run_dir=run_dir,
                run_id=run_id,
                round_id=next_round_id,
                skill_name="eco-open-investigation-round",
                contract_mode=contract_mode,
                skill_args=["--source-round-id", round_id],
            ),
        },
    ]


def operator_notes(
    *,
    status: str,
    mission: dict[str, Any],
    advisory_plan: dict[str, Any],
    round_surface_payload: dict[str, Any],
    analysis: dict[str, Any],
) -> list[str]:
    notes = [
        "Agent entry remains advisory-first: direct reads and board writes can happen through governed skills without replacing runtime hard gates.",
        "Promotion, archive, replay, and publication stay outside the agent inner loop and must return to runtime kernel commands.",
    ]
    if maybe_text(mission.get("orchestration_mode")) == "openclaw-agent":
        notes.append("Mission scaffold already marks this round as `openclaw-agent`, so the operator-visible entry chain is explicitly enabled.")
    if advisory_plan.get("present"):
        notes.append(
            f"Advisory plan posture is `{maybe_text(advisory_plan.get('downstream_posture')) or 'unspecified'}` with primary role `{maybe_text(advisory_plan.get('primary_role')) or 'moderator'}`."
        )
    if int(analysis.get("matching_result_set_count") or 0) > 0:
        notes.append(
            f"Analysis plane currently exposes {int(analysis.get('matching_result_set_count') or 0)} latest result sets for this round."
        )
    if maybe_text(round_surface_payload.get("state_source")) == "deliberation-plane":
        notes.append("Board state is already readable from the deliberation plane, so agent-side context does not depend on `board_summary` or `board_brief` artifacts.")
    if status == "needs-operator-review":
        notes.append("Resolve runtime health alerts or dead letters before trusting agent-guided next steps.")
    return notes[:5]


def build_agent_entry_payload(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    contract_mode: str,
) -> dict[str, Any]:
    governance = governance_surface(run_dir, round_id=round_id)
    mission = mission_surface(run_dir, round_id)
    advisory_plan = advisory_plan_surface(run_dir, round_id)
    round_state = round_surface(run_dir, run_id=run_id, round_id=round_id)
    analysis = analysis_surface(run_dir, run_id=run_id, round_id=round_id)
    next_round_id = suggest_next_round_id(run_dir, round_id)
    status, warnings = entry_status(
        governance=governance,
        mission=mission,
        round_surface_payload=round_state,
        analysis=analysis,
    )
    role_entries = role_entry_points(
        run_dir=run_dir,
        run_id=run_id,
        round_id=round_id,
        contract_mode=contract_mode,
        next_round_id=next_round_id,
    )
    recommended_skills = unique_texts(
        [
            *(
                advisory_plan.get("recommended_skill_sequence", [])
                if isinstance(advisory_plan.get("recommended_skill_sequence"), list)
                else []
            ),
            "eco-read-board-delta",
            "eco-query-public-signals",
            "eco-query-environment-signals",
            "eco-post-board-note",
            "eco-update-hypothesis-status",
            "eco-open-challenge-ticket",
            "eco-open-falsification-probe",
        ]
    )
    payload = {
        "schema_version": "runtime-agent-entry-gate-v1",
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "entry_id": "agent-entry-" + new_runtime_event_id("gate", run_id, round_id, status).split("-", 1)[1],
        "entry_status": status,
        "orchestration_mode": maybe_text(mission.get("orchestration_mode")) or "openclaw-agent-compatible",
        "contract_mode": contract_mode,
        "output_path": str(agent_entry_gate_path(run_dir, round_id).resolve()),
        "mission": mission,
        "governance": governance,
        "round_surface": round_state,
        "analysis_surface": analysis,
        "advisory_plan": advisory_plan,
        "recommended_entry_skills": recommended_skills,
        "role_entry_points": role_entries,
        "entry_chain": entry_chain(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            contract_mode=contract_mode,
            next_round_id=next_round_id,
        ),
        "hard_gate_commands": hard_gate_commands(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            next_round_id=next_round_id,
            contract_mode=contract_mode,
        ),
        "operator_notes": operator_notes(
            status=status,
            mission=mission,
            advisory_plan=advisory_plan,
            round_surface_payload=round_state,
            analysis=analysis,
        ),
        "warnings": warnings
        + (
            analysis.get("warnings", [])
            if isinstance(analysis.get("warnings"), list)
            else []
        ),
    }
    return payload


def agent_entry_operator_view(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    gate_payload: dict[str, Any] | None,
    contract_mode: str = "warn",
) -> dict[str, Any]:
    gate = gate_payload if isinstance(gate_payload, dict) else {}
    next_round_id = suggest_next_round_id(run_dir, round_id) if round_id else ""
    return {
        "entry_gate_present": bool(gate),
        "entry_status": maybe_text(gate.get("entry_status")) or "",
        "orchestration_mode": maybe_text(gate.get("orchestration_mode")) or "",
        "entry_gate_path": str(agent_entry_gate_path(run_dir, round_id).resolve()) if round_id else "",
        "mission_scaffold_path": str(mission_scaffold_path(run_dir, round_id).resolve()) if round_id else "",
        "agent_advisory_plan_path": str(agent_advisory_plan_path(run_dir, round_id).resolve()) if round_id else "",
        "recommended_entry_skills": gate.get("recommended_entry_skills", []) if isinstance(gate.get("recommended_entry_skills"), list) else [],
        "materialize_agent_entry_gate_command": kernel_command(
            "materialize-agent-entry-gate",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--pretty",
        )
        if run_id and round_id
        else "",
        "refresh_agent_entry_gate_command": kernel_command(
            "materialize-agent-entry-gate",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--refresh-advisory-plan",
            "--pretty",
        )
        if run_id and round_id
        else "",
        "materialize_agent_advisory_plan_command": advisory_plan_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            contract_mode=contract_mode,
        )
        if run_id and round_id
        else "",
        "read_board_delta_command": run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="eco-read-board-delta",
            contract_mode=contract_mode,
            skill_args=["--include-closed", "--event-limit", "20"],
        )
        if run_id and round_id
        else "",
        "query_public_signals_command": run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="eco-query-public-signals",
            contract_mode=contract_mode,
        )
        if run_id and round_id
        else "",
        "query_environment_signals_command": run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="eco-query-environment-signals",
            contract_mode=contract_mode,
        )
        if run_id and round_id
        else "",
        "list_claim_cluster_result_sets_command": query_result_set_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            analysis_kind="claim-cluster",
        )
        if run_id and round_id
        else "",
        "query_claim_cluster_items_command_template": query_result_item_template(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            analysis_kind="claim-cluster",
        )
        if run_id and round_id
        else "",
        "open_next_round_command_template": run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=next_round_id,
            skill_name="eco-open-investigation-round",
            contract_mode=contract_mode,
            skill_args=["--source-round-id", round_id],
        )
        if run_id and round_id and next_round_id
        else "",
        "return_to_supervisor_command": kernel_command(
            "supervise-round",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--pretty",
        )
        if run_id and round_id
        else "",
    }


def agent_entry_state(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    contract_mode: str = "warn",
) -> dict[str, Any]:
    if not round_id:
        return {}
    gate = load_json_if_exists(agent_entry_gate_path(run_dir, round_id)) or {}
    return {
        "gate": gate,
        "operator": agent_entry_operator_view(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            gate_payload=gate,
            contract_mode=contract_mode,
        ),
    }


def materialize_agent_entry_gate(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    contract_mode: str = "warn",
    refresh_advisory_plan: bool = False,
    timeout_seconds: float | None = None,
    retry_budget: int | None = None,
    retry_backoff_ms: int | None = None,
    allow_side_effects: list[str] | None = None,
) -> dict[str, Any]:
    resolved_run_dir = resolve_run_dir(run_dir)
    initial_payload = build_agent_entry_payload(
        resolved_run_dir,
        run_id=run_id,
        round_id=round_id,
        contract_mode=contract_mode,
    )
    advisory_plan_file = agent_advisory_plan_path(resolved_run_dir, round_id)
    advisory_plan_materialized = False
    advisory_plan_receipt_id = ""
    if maybe_text(initial_payload.get("entry_status")) != "blocked" and (
        refresh_advisory_plan or not advisory_plan_file.exists()
    ):
        plan_result = run_skill(
            resolved_run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="eco-plan-round-orchestration",
            skill_args=[
                "--planner-mode",
                "agent-advisory",
                "--output-path",
                advisory_plan_relative_path(resolved_run_dir, round_id),
            ],
            contract_mode=contract_mode,
            timeout_seconds=timeout_seconds,
            retry_budget=retry_budget,
            retry_backoff_ms=retry_backoff_ms,
            allow_side_effects=allow_side_effects,
        )
        advisory_plan_materialized = True
        advisory_plan_receipt_id = maybe_text(plan_result.get("summary", {}).get("receipt_id"))
    payload = build_agent_entry_payload(
        resolved_run_dir,
        run_id=run_id,
        round_id=round_id,
        contract_mode=contract_mode,
    )
    output_file = agent_entry_gate_path(resolved_run_dir, round_id)
    write_json(output_file, payload)
    append_ledger_event(
        resolved_run_dir,
        {
            "schema_version": "runtime-event-v3",
            "event_id": new_runtime_event_id(
                "runtimeevt",
                run_id,
                round_id,
                "agent-entry-gate",
                payload.get("generated_at_utc"),
                payload.get("entry_status"),
            ),
            "event_type": "agent-entry-gate",
            "run_id": run_id,
            "round_id": round_id,
            "started_at_utc": payload.get("generated_at_utc"),
            "completed_at_utc": payload.get("generated_at_utc"),
            "status": "completed",
            "entry_status": payload.get("entry_status"),
            "orchestration_mode": payload.get("orchestration_mode"),
            "agent_entry_gate_path": str(output_file.resolve()),
            "agent_advisory_plan_path": str(advisory_plan_file.resolve()),
            "advisory_plan_materialized": advisory_plan_materialized,
            "advisory_plan_receipt_id": advisory_plan_receipt_id,
        },
    )
    return {
        "status": "completed",
        "summary": {
            "run_dir": str(resolved_run_dir),
            "run_id": run_id,
            "round_id": round_id,
            "entry_status": maybe_text(payload.get("entry_status")),
            "orchestration_mode": maybe_text(payload.get("orchestration_mode")),
            "output_path": str(output_file.resolve()),
            "advisory_plan_path": str(advisory_plan_file.resolve()),
            "advisory_plan_present": bool(payload.get("advisory_plan", {}).get("present"))
            if isinstance(payload.get("advisory_plan"), dict)
            else False,
            "advisory_plan_materialized": advisory_plan_materialized,
            "analysis_kind_count": int(payload.get("analysis_surface", {}).get("analysis_kind_count") or 0)
            if isinstance(payload.get("analysis_surface"), dict)
            else 0,
            "recommended_skill_count": len(payload.get("recommended_entry_skills", []))
            if isinstance(payload.get("recommended_entry_skills"), list)
            else 0,
            "role_count": len(payload.get("role_entry_points", []))
            if isinstance(payload.get("role_entry_points"), list)
            else 0,
        },
        "agent_entry": payload,
    }


__all__ = [
    "agent_entry_state",
    "materialize_agent_entry_gate",
]
