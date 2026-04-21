from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Callable

from .kernel.executor import maybe_text, run_skill
from .kernel.paths import agent_advisory_plan_path
from .phase2_direct_advisory import materialize_direct_council_advisory_plan
from .phase2_planning_profile import planner_skill_args_for_source
from .phase2_round_profile import default_next_round_id_builder
from .phase2_stage_profile import DEFAULT_PHASE2_PLANNER_SKILL_NAME
from .runtime_command_hints import kernel_command, run_skill_command

EntryStatusEvaluator = Callable[..., tuple[str, list[dict[str, str]]]]
RoleEntryBuilder = Callable[..., list[dict[str, Any]]]
RecommendedSkillsBuilder = Callable[..., list[str]]
OperatorNotesBuilder = Callable[..., list[str]]
OperatorCommandsBuilder = Callable[..., dict[str, str]]


DEFAULT_AGENT_ENTRY_ROLE_DEFINITIONS = [
    {
        "role": "sociologist",
        "focus": "public/formal evidence query, narrative regrouping, and claim-side analysis.",
        "read_skills": [
            "eco-read-board-delta",
            "eco-query-public-signals",
            "eco-query-formal-signals",
        ],
        "write_skills": [
            "eco-submit-council-proposal",
            "eco-submit-readiness-opinion",
            "eco-update-hypothesis-status",
        ],
        "analysis_kinds": ["claim-candidate", "claim-cluster", "claim-scope", "evidence-coverage"],
    },
    {
        "role": "environmentalist",
        "focus": "environment evidence query, observation analysis, and corroboration work.",
        "read_skills": ["eco-read-board-delta", "eco-query-environment-signals"],
        "write_skills": [
            "eco-submit-council-proposal",
            "eco-submit-readiness-opinion",
            "eco-update-hypothesis-status",
        ],
        "analysis_kinds": ["observation-candidate", "merged-observation", "observation-scope", "evidence-coverage"],
    },
    {
        "role": "challenger",
        "focus": "contradiction pressure, challenge tickets, and falsification probes.",
        "read_skills": [
            "eco-read-board-delta",
            "eco-query-public-signals",
            "eco-query-formal-signals",
            "eco-query-environment-signals",
        ],
        "write_skills": [
            "eco-submit-council-proposal",
            "eco-submit-readiness-opinion",
            "eco-open-challenge-ticket",
            "eco-open-falsification-probe",
            "eco-close-challenge-ticket",
        ],
        "analysis_kinds": ["claim-cluster", "merged-observation", "evidence-coverage"],
    },
    {
        "role": "moderator",
        "focus": "board state progression, formal/public route review, and return to runtime hard gates.",
        "read_skills": [
            "eco-read-board-delta",
            "eco-query-formal-signals",
            "eco-query-public-signals",
        ],
        "write_skills": [
            "eco-submit-council-proposal",
            "eco-submit-readiness-opinion",
            "eco-claim-board-task",
            "eco-open-investigation-round",
        ],
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


def advisory_plan_relative_path(run_dir: Path, round_id: str) -> str:
    return str(agent_advisory_plan_path(run_dir, round_id).relative_to(run_dir))


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


def advisory_plan_command(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
) -> str:
    return kernel_command(
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


def default_agent_entry_status(
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


def default_role_entry_points(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    contract_mode: str,
    next_round_id: str,
    role_definitions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for definition in role_definitions:
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
            for analysis_kind in definition.get("analysis_kinds", [])
            if isinstance(definition.get("analysis_kinds"), list)
        ]
        role_write_commands: list[str] = []
        for skill_name in definition.get("write_skills", []) if isinstance(definition.get("write_skills"), list) else []:
            if skill_name == "eco-submit-council-proposal":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        contract_mode=contract_mode,
                        skill_args=[
                            "--agent-role",
                            role,
                            "--proposal-kind",
                            "<proposal_kind>",
                            "--rationale",
                            "<rationale>",
                            "--decision-source",
                            "agent-council",
                            "--target-kind",
                            "<target_kind>",
                            "--target-id",
                            "<target_id>",
                        ],
                    )
                )
            elif skill_name == "eco-submit-readiness-opinion":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        contract_mode=contract_mode,
                        skill_args=[
                            "--agent-role",
                            role,
                            "--readiness-status",
                            "<ready|needs-more-data|blocked>",
                            "--rationale",
                            "<rationale>",
                            "--basis-object-id",
                            "<basis_object_id>",
                        ],
                    )
                )
            elif skill_name == "eco-post-board-note":
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


def default_agent_entry_recommended_skills(*, advisory_plan: dict[str, Any]) -> list[str]:
    return unique_texts(
        [
            *(
                advisory_plan.get("recommended_skill_sequence", [])
                if isinstance(advisory_plan.get("recommended_skill_sequence"), list)
                else []
            ),
            "eco-read-board-delta",
            "eco-query-public-signals",
            "eco-query-environment-signals",
            "eco-submit-council-proposal",
            "eco-submit-readiness-opinion",
            "eco-update-hypothesis-status",
            "eco-open-challenge-ticket",
            "eco-open-falsification-probe",
        ]
    )


def default_agent_entry_operator_notes(
    *,
    status: str,
    mission: dict[str, Any],
    advisory_plan: dict[str, Any],
    round_surface_payload: dict[str, Any],
    analysis: dict[str, Any],
) -> list[str]:
    notes = [
        "Agent entry remains advisory-first: direct reads and structured proposal/readiness writes can happen through governed skills without replacing runtime hard gates.",
        "Promotion, archive, replay, and publication stay outside the agent inner loop and must return to runtime kernel commands.",
    ]
    if maybe_text(mission.get("orchestration_mode")) == "openclaw-agent":
        notes.append("Mission scaffold already marks this round as `openclaw-agent`, so the operator-visible entry chain is explicitly enabled.")
    if advisory_plan.get("present"):
        notes.append(
            f"Advisory plan posture is `{maybe_text(advisory_plan.get('downstream_posture')) or 'unspecified'}` with primary role `{maybe_text(advisory_plan.get('primary_role')) or 'moderator'}`."
        )
        if maybe_text(advisory_plan.get("plan_source")) == "direct-council-advisory":
            notes.append("Current advisory queue was compiled directly from DB-backed council objects instead of a planner skill subprocess.")
    if int(analysis.get("matching_result_set_count") or 0) > 0:
        notes.append(
            f"Analysis plane currently exposes {int(analysis.get('matching_result_set_count') or 0)} latest result sets for this round."
        )
    if maybe_text(round_surface_payload.get("state_source")) == "deliberation-plane":
        notes.append("Board state is already readable from the deliberation plane, so agent-side context does not depend on `board_summary` or `board_brief` artifacts.")
        notes.append("Default write path should now prefer `proposal / readiness-opinion` submissions over freeform board notes whenever the agent is making a council judgement.")
    if status == "needs-operator-review":
        notes.append("Resolve runtime health alerts or dead letters before trusting agent-guided next steps.")
    return notes[:5]


def default_agent_entry_operator_commands(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    contract_mode: str,
) -> dict[str, str]:
    if not run_id or not round_id:
        return {}
    return {
        "materialize_agent_entry_gate_command": kernel_command(
            "materialize-agent-entry-gate",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--pretty",
        ),
        "refresh_agent_entry_gate_command": advisory_plan_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
        ),
        "materialize_agent_advisory_plan_command": advisory_plan_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
        ),
        "read_board_delta_command": run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="eco-read-board-delta",
            contract_mode=contract_mode,
            skill_args=["--include-closed", "--event-limit", "20"],
        ),
        "query_public_signals_command": run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="eco-query-public-signals",
            contract_mode=contract_mode,
        ),
        "query_formal_signals_command": run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="eco-query-formal-signals",
            contract_mode=contract_mode,
        ),
        "query_environment_signals_command": run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="eco-query-environment-signals",
            contract_mode=contract_mode,
        ),
        "query_council_proposals_command": kernel_command(
            "query-council-objects",
            "--run-dir",
            str(run_dir),
            "--object-kind",
            "proposal",
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--include-contract",
            "--pretty",
        ),
        "query_readiness_opinions_command": kernel_command(
            "query-council-objects",
            "--run-dir",
            str(run_dir),
            "--object-kind",
            "readiness-opinion",
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--include-contract",
            "--pretty",
        ),
        "submit_council_proposal_command_template": run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="eco-submit-council-proposal",
            contract_mode=contract_mode,
            skill_args=[
                "--agent-role",
                "<agent_role>",
                "--proposal-kind",
                "<proposal_kind>",
                "--rationale",
                "<rationale>",
                "--target-kind",
                "<target_kind>",
                "--target-id",
                "<target_id>",
            ],
        ),
        "submit_readiness_opinion_command_template": run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="eco-submit-readiness-opinion",
            contract_mode=contract_mode,
            skill_args=[
                "--agent-role",
                "<agent_role>",
                "--readiness-status",
                "<ready|needs-more-data|blocked>",
                "--rationale",
                "<rationale>",
                "--basis-object-id",
                "<basis_object_id>",
            ],
        ),
        "list_claim_cluster_result_sets_command": query_result_set_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            analysis_kind="claim-cluster",
        ),
        "query_claim_cluster_items_command_template": query_result_item_template(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            analysis_kind="claim-cluster",
        ),
    }


def agent_entry_advisory_source(
    source_name: str,
    *,
    source_kind: str,
    planner_mode: str = "",
    planner_skill_name: str = DEFAULT_PHASE2_PLANNER_SKILL_NAME,
) -> dict[str, Any]:
    return {
        "source_name": maybe_text(source_name),
        "source_kind": maybe_text(source_kind),
        "planner_mode": maybe_text(planner_mode),
        "planner_skill_name": maybe_text(planner_skill_name) or DEFAULT_PHASE2_PLANNER_SKILL_NAME,
        "output_path_key": "agent_advisory_plan_path",
    }


def default_agent_entry_advisory_sources(
    *,
    planner_skill_name: str = DEFAULT_PHASE2_PLANNER_SKILL_NAME,
) -> list[dict[str, Any]]:
    return [
        agent_entry_advisory_source(
            "direct-council-advisory",
            source_kind="direct-council-advisory",
            planner_skill_name=planner_skill_name,
        ),
        agent_entry_advisory_source(
            "agent-advisory",
            source_kind="planner-skill",
            planner_mode="agent-advisory",
            planner_skill_name=planner_skill_name,
        ),
    ]


def materialize_agent_entry_advisory_plan(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    contract_mode: str,
    advisory_sources: list[dict[str, Any]],
    timeout_seconds: float | None = None,
    retry_budget: int | None = None,
    retry_backoff_ms: int | None = None,
    allow_side_effects: list[str] | None = None,
) -> dict[str, Any]:
    output_path = agent_advisory_plan_path(run_dir, round_id)
    for source_spec in advisory_sources:
        source_kind = maybe_text(source_spec.get("source_kind"))
        source_name = maybe_text(source_spec.get("source_name"))
        if source_kind == "direct-council-advisory":
            try:
                result = materialize_direct_council_advisory_plan(
                    run_dir,
                    run_id=run_id,
                    round_id=round_id,
                    output_path=advisory_plan_relative_path(run_dir, round_id),
                    contract_mode=contract_mode,
                )
            except Exception:
                continue
            if isinstance(result, dict) and result:
                summary = result.get("summary", {}) if isinstance(result.get("summary"), dict) else {}
                skill_payload = result.get("skill_payload", {}) if isinstance(result.get("skill_payload"), dict) else {}
                return {
                    "materialized": True,
                    "receipt_id": maybe_text(summary.get("receipt_id")),
                    "source": maybe_text(skill_payload.get("plan_source")) or source_name,
                }
            continue
        if source_kind != "planner-skill":
            continue
        planner_result = run_skill(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name=maybe_text(source_spec.get("planner_skill_name")) or DEFAULT_PHASE2_PLANNER_SKILL_NAME,
            skill_args=planner_skill_args_for_source(run_dir, source_spec, output_path),
            contract_mode=contract_mode,
            timeout_seconds=timeout_seconds,
            retry_budget=retry_budget,
            retry_backoff_ms=retry_backoff_ms,
            allow_side_effects=allow_side_effects,
        )
        summary = planner_result.get("summary", {}) if isinstance(planner_result.get("summary"), dict) else {}
        skill_payload = planner_result.get("skill_payload", {}) if isinstance(planner_result.get("skill_payload"), dict) else {}
        return {
            "materialized": True,
            "receipt_id": maybe_text(summary.get("receipt_id")),
            "source": maybe_text(skill_payload.get("plan_source")) or source_name,
        }
    return {
        "materialized": False,
        "receipt_id": "",
        "source": "",
    }


def default_phase2_agent_entry_profile(
    *,
    planner_skill_name: str = DEFAULT_PHASE2_PLANNER_SKILL_NAME,
) -> dict[str, Any]:
    return {
        "role_definitions": deepcopy(DEFAULT_AGENT_ENTRY_ROLE_DEFINITIONS),
        "status_evaluator": default_agent_entry_status,
        "next_round_id_builder": default_next_round_id_builder,
        "role_entry_builder": default_role_entry_points,
        "recommended_skills_builder": default_agent_entry_recommended_skills,
        "operator_notes_builder": default_agent_entry_operator_notes,
        "operator_commands_builder": default_agent_entry_operator_commands,
        "advisory_sources": deepcopy(
            default_agent_entry_advisory_sources(
                planner_skill_name=planner_skill_name,
            )
        ),
    }


__all__ = [
    "EntryStatusEvaluator",
    "OperatorCommandsBuilder",
    "OperatorNotesBuilder",
    "RecommendedSkillsBuilder",
    "RoleEntryBuilder",
    "advisory_plan_command",
    "default_agent_entry_advisory_sources",
    "default_agent_entry_operator_commands",
    "default_agent_entry_operator_notes",
    "default_agent_entry_recommended_skills",
    "default_agent_entry_status",
    "default_phase2_agent_entry_profile",
    "default_role_entry_points",
    "materialize_agent_entry_advisory_plan",
]
