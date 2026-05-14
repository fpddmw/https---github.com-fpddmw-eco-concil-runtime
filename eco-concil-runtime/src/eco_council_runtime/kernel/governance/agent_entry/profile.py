from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable

from eco_council_runtime.kernel.execution.executor import maybe_text
from eco_council_runtime.kernel.governance.claim_strength import claim_strength_obligations
from eco_council_runtime.kernel.governance.role_contracts import (
    ROLE_CHALLENGER,
    ROLE_ENVIRONMENTAL_INVESTIGATOR,
    ROLE_MODERATOR,
    ROLE_REPORT_EDITOR,
    ROLE_SOCIAL_INVESTIGATOR,
    role_contract,
)
from eco_council_runtime.kernel.governance.skill_registry import (
    SKILL_LAYER_DELIBERATION_WRITE,
    SKILL_LAYER_FETCH,
    SKILL_LAYER_NORMALIZE,
    SKILL_LAYER_OPTIONAL_ANALYSIS,
    SKILL_LAYER_QUERY,
    SKILL_LAYER_REPORTING,
    SKILL_LAYER_STATE_TRANSITION,
    available_skill_names,
    resolve_skill_policy,
)
from eco_council_runtime.kernel.governance.transition_requests import (
    TRANSITION_KIND_CLOSE_ROUND,
    TRANSITION_KIND_FREEZE_REPORT_BASIS,
    TRANSITION_KIND_OPEN_INVESTIGATION_ROUND,
    TRANSITION_KIND_OPEN_REPORT_WRITING_ROUND,
)
from eco_council_runtime.kernel.execution.runtime_round_profile import default_next_round_id_builder
from eco_council_runtime.kernel.source_queue.source_queue_contract import source_capability_hints
from eco_council_runtime.kernel.source_queue.source_queue_profile import (
    source_family_workflows_for_skills,
    source_queue_profile,
)
from eco_council_runtime.runtime_command_hints import kernel_command, run_skill_command

EntryStatusEvaluator = Callable[..., tuple[str, list[dict[str, str]]]]
RoleEntryBuilder = Callable[..., list[dict[str, Any]]]
RecommendedSkillsBuilder = Callable[..., list[str]]
OperatorNotesBuilder = Callable[..., list[str]]
OperatorCommandsBuilder = Callable[..., dict[str, str]]

REPORT_WRITING_ROUND_MODES = {
    "report-writing",
    "reporting",
    "narrative-report",
    "narrative-reporting",
}

COORDINATION_READ_OBJECT_KINDS = (
    "investigation-plan",
    "subissue",
    "investigation-scope",
    "round-brief",
    "round-synthesis",
    "evidence-request",
    "source-acquisition-proposal",
    "agent-position",
    "context-packet",
    "challenge-disposition",
)


DEFAULT_AGENT_ENTRY_ROLE_DEFINITIONS = [
    {
        "role": ROLE_MODERATOR,
        "focus": "Own the study boundary, review cross-role findings, and file governed transition requests after human-auditable council deliberation.",
        "role_boundary_guidance": {
            "schema_version": "role-boundary-guidance-v1",
            "claim_boundary_focus": [
                "Organize agenda, scope, and transition proposals without deciding source sufficiency by runtime fiat.",
                "Keep evidence adoption, limitations, and report readiness council-visible and challengeable.",
            ],
            "coordination_expectations": [
                "Use investigator positions, challenger review, and recorded limitations before requesting phase transitions.",
                "Keep optional public-discourse deepening as a council-adopted lane, not a mandatory runtime sequence.",
            ],
        },
        "read_skills": [
            "query-board-delta",
            "query-public-signals",
            "query-formal-signals",
            "query-environment-signals",
        ],
        "write_skills": [
            "submit-investigation-plan",
            "submit-investigation-scope",
            "submit-round-brief",
            "submit-round-synthesis",
            "materialize-context-packet",
            "submit-evidence-request",
            "update-source-acquisition-proposal-status",
            "link-source-acquisition-execution",
            "submit-agent-position",
            "update-hypothesis-status",
            "submit-challenge-disposition",
            "submit-council-proposal",
            "submit-readiness-opinion",
            "claim-board-task",
            "post-board-note",
        ],
        "analysis_kinds": [],
        "transition_kinds": [
            TRANSITION_KIND_FREEZE_REPORT_BASIS,
            TRANSITION_KIND_OPEN_INVESTIGATION_ROUND,
            TRANSITION_KIND_OPEN_REPORT_WRITING_ROUND,
            TRANSITION_KIND_CLOSE_ROUND,
        ],
    },
    {
        "role": ROLE_ENVIRONMENTAL_INVESTIGATOR,
        "focus": "Fetch, normalize, query, and analyze environmental evidence, then submit structured findings or challenge supporting basis back to the council.",
        "role_boundary_guidance": {
            "schema_version": "role-boundary-guidance-v1",
            "claim_boundary_focus": [
                "Separate environmental compatibility, timing, and receptor-side conditions from specific physical source attribution.",
                "Treat source-specific attribution as a strong claim requiring trajectory, plume, chemistry, professional attribution model, or equivalent council-visible support.",
                "Do not convert public, media, or formal-record source narratives into physical source proof without environmental verification.",
            ],
            "coordination_expectations": [
                "Respond to social or formal source-narrative cues with compatibility, limitation, or verification status.",
                "Submit explicit limitations when the basis supports conditions or associations but not causal transport or source proof.",
            ],
        },
        "read_skills": [
            "query-board-delta",
            "query-environment-signals",
            "query-formal-signals",
        ],
        "write_skills": [
            "submit-investigation-scope",
            "submit-evidence-request",
            "submit-source-acquisition-proposal",
            "update-source-acquisition-proposal-status",
            "link-source-acquisition-execution",
            "submit-agent-position",
            "update-hypothesis-status",
            "submit-council-proposal",
            "submit-readiness-opinion",
            "post-board-note",
        ],
        "analysis_kinds": [],
    },
    {
        "role": ROLE_SOCIAL_INVESTIGATOR,
        "focus": "Fetch, normalize, query, and analyze public discourse, community, formal record, and policy evidence. For affect/stance labels, use bounded annotation-worker artifacts and own sample/basis/uptake rather than personally authoring every label.",
        "role_boundary_guidance": {
            "schema_version": "role-boundary-guidance-v1",
            "claim_boundary_focus": [
                "Describe public visibility, issue cues, affect cues, source-narrative cues, and sample boundaries without upgrading samples into representative public opinion.",
                "Use bounded annotation-worker artifacts and taxonomy checks for affect or stance labels; own the sample, basis, and uptake into council findings.",
                "Treat source narratives in public discourse as claims to route for environmental verification, not physical source attribution.",
            ],
            "coordination_expectations": [
                "Ask environmental-investigator to verify whether public source narratives are physically supported.",
                "Mark sample-local patterns, platform or provider limits, and optional analysis status before report handoff.",
            ],
        },
        "read_skills": [
            "query-board-delta",
            "query-public-signals",
            "query-formal-signals",
        ],
        "write_skills": [
            "submit-investigation-scope",
            "submit-evidence-request",
            "submit-source-acquisition-proposal",
            "update-source-acquisition-proposal-status",
            "link-source-acquisition-execution",
            "submit-agent-position",
            "update-hypothesis-status",
            "submit-council-proposal",
            "submit-readiness-opinion",
            "post-board-note",
        ],
        "analysis_kinds": [],
    },
    {
        "role": ROLE_CHALLENGER,
        "focus": "Surface contradiction pressure, open challenge/probe work, and submit counter-findings without owning phase transitions. For public-discourse labels, review sample boundaries, taxonomy fit, ambiguous clusters, outliers, and report wording rather than relabeling every item.",
        "role_boundary_guidance": {
            "schema_version": "role-boundary-guidance-v1",
            "claim_boundary_focus": [
                "Review claim boundary, sample boundary, taxonomy fit, outliers, extrapolation risk, and report wording.",
                "Challenge causal attribution, representative public-opinion claims, and policy conclusions when the recorded basis does not support them.",
                "For non-GDELT public-discourse material, pressure-test sample and labeling logic rather than relabeling every item.",
            ],
            "coordination_expectations": [
                "Open challenge tickets or falsification probes when stronger wording needs new evidence.",
                "Record whether limitations are sufficient, need follow-up, or should be excluded from report basis.",
            ],
        },
        "read_skills": [
            "query-board-delta",
            "query-public-signals",
            "query-formal-signals",
            "query-environment-signals",
        ],
        "write_skills": [
            "submit-investigation-scope",
            "submit-evidence-request",
            "submit-source-acquisition-proposal",
            "update-source-acquisition-proposal-status",
            "link-source-acquisition-execution",
            "submit-agent-position",
            "submit-council-proposal",
            "submit-readiness-opinion",
            "post-board-note",
            "post-review-comment",
            "submit-challenge-disposition",
            "open-challenge-ticket",
            "open-falsification-probe",
            "close-challenge-ticket",
        ],
        "analysis_kinds": [],
    },
    {
        "role": ROLE_REPORT_EDITOR,
        "focus": "Read frozen evidence basis and reporting state, then draft or publish reporting artifacts without mutating investigation status.",
        "role_boundary_guidance": {
            "schema_version": "role-boundary-guidance-v1",
            "claim_boundary_focus": [
                "Consume frozen evidence basis, reporting handoff, and council-adopted limitations only; do not reopen investigation through report prose.",
                "Distinguish sample-local discourse structure from representative public opinion.",
                "Distinguish public source narratives from physical source attribution and keep unsupported causal or policy claims out of final reports.",
            ],
            "coordination_expectations": [
                "Surface missing challenger review, public-discourse carry, or environmental attribution support as limitations rather than conclusions.",
                "Use narrative validation before publication when public, attribution, or policy wording appears in the draft.",
            ],
        },
        "read_skills": [
            "query-board-delta",
            "query-formal-signals",
            "query-public-signals",
            "query-environment-signals",
        ],
        "write_skills": [
            "materialize-reporting-handoff",
            "draft-council-decision",
            "draft-expert-report",
            "draft-narrative-report",
            "validate-narrative-report",
            "publish-narrative-report",
            "publish-expert-report",
            "publish-council-decision",
            "materialize-final-publication",
        ],
        "analysis_kinds": [],
    },
]


def skill_use_discipline() -> dict[str, Any]:
    return {
        "schema_version": "agent-skill-use-discipline-v1",
        "purpose": (
            "Keep agents autonomous while preventing tool mistakes from becoming "
            "overconfident council conclusions."
        ),
        "core_principles": [
            "Skills expose capability surfaces; they do not decide truth, source sufficiency, report readiness, or evidence acceptance.",
            "A zero, failed, blocked, no-op, or receipt-only skill result is an attempt result, not proof that no real-world evidence exists.",
            "Before saying data cannot be found or evidence cannot be combined, explain the skill, parameters, coverage, and unresolved alternatives in one concise sentence.",
            "Use same-family follow-up skills, preflight commands, linting, metadata, availability checks, or revised parameters when the evidence need remains live.",
            "Weak or bounded reports are allowed only after the moderator records limitations and why live actionable routes are not being continued.",
        ],
        "lightweight_negative_claim_template": (
            "Under <skill> with <query/window/bbox/provider-mode>, this attempt returned "
            "<zero/failed/receipt-only>. This does not rule out <untried routes>; next "
            "I will <revise/switch/ask moderator/bound the claim>."
        ),
        "autonomy_boundary": (
            "This is not a form requirement, source ranking, evidence weighting, or "
            "fixed agenda. It is a short reasoning discipline for moments when a "
            "tool result might otherwise be mistaken for a research conclusion."
        ),
    }


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


def load_json_if_exists(path: Path) -> Any | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def round_opening_mode(run_dir: Path, round_id: str) -> str:
    transition = load_json_if_exists(run_dir / "runtime" / f"round_transition_{round_id}.json")
    if isinstance(transition, dict):
        mode = maybe_text(transition.get("round_mode"))
        if mode:
            return mode
        context = transition.get("coordination_context")
        if isinstance(context, dict):
            mode = maybe_text(context.get("round_mode"))
            if mode:
                return mode

    tasks = load_json_if_exists(run_dir / "investigation" / f"round_tasks_{round_id}.json")
    if isinstance(tasks, list):
        for task in tasks:
            if not isinstance(task, dict):
                continue
            inputs = task.get("inputs") if isinstance(task.get("inputs"), dict) else {}
            context = (
                inputs.get("round_coordination_context")
                if isinstance(inputs.get("round_coordination_context"), dict)
                else {}
            )
            mode = maybe_text(context.get("round_mode")) or maybe_text(task.get("round_mode"))
            if mode:
                return mode
    return ""


def role_definitions_for_round(
    *,
    run_dir: Path,
    round_id: str,
    role_definitions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    mode = round_opening_mode(run_dir, round_id).casefold()
    if mode not in REPORT_WRITING_ROUND_MODES:
        return role_definitions
    return [
        deepcopy(definition)
        for definition in role_definitions
        if maybe_text(definition.get("role")) == ROLE_REPORT_EDITOR
    ]


def allowed_skills_by_layer(role: str) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {}
    for skill_name in available_skill_names():
        policy = resolve_skill_policy(skill_name)
        allowed_roles = (
            policy.get("allowed_roles", [])
            if isinstance(policy.get("allowed_roles"), list)
            else []
        )
        if role not in allowed_roles:
            continue
        layer = maybe_text(policy.get("skill_layer")) or "unknown"
        grouped.setdefault(layer, []).append(skill_name)
    return {layer: sorted(skill_names) for layer, skill_names in grouped.items()}


def skill_count_by_layer(role: str) -> dict[str, int]:
    grouped = allowed_skills_by_layer(role)
    return {layer: len(skill_names) for layer, skill_names in grouped.items()}


def capability_layers(role: str) -> list[str]:
    grouped = allowed_skills_by_layer(role)
    ordered_layers = [
        SKILL_LAYER_FETCH,
        SKILL_LAYER_NORMALIZE,
        SKILL_LAYER_QUERY,
        SKILL_LAYER_OPTIONAL_ANALYSIS,
        SKILL_LAYER_DELIBERATION_WRITE,
        SKILL_LAYER_STATE_TRANSITION,
        SKILL_LAYER_REPORTING,
    ]
    return [
        layer
        for layer in ordered_layers
        if layer in grouped
    ] + [layer for layer in sorted(grouped) if layer not in ordered_layers]


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


def query_coordination_object_command(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    object_kind: str,
) -> str:
    return kernel_command(
        "query-council-objects",
        "--run-dir",
        str(run_dir),
        "--object-kind",
        object_kind,
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--include-contract",
        "--pretty",
    )


def coordination_read_commands(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
) -> list[str]:
    return [
        query_coordination_object_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            object_kind=object_kind,
        )
        for object_kind in COORDINATION_READ_OBJECT_KINDS
    ]


def layer_skill_commands(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    contract_mode: str,
    actor_role: str,
    skill_names: list[str],
    command_kind: str,
) -> list[str]:
    commands: list[str] = []
    for skill_name in skill_names:
        if command_kind == SKILL_LAYER_FETCH:
            capability_hints = source_capability_hints(skill_name)
            templates = (
                capability_hints.get("fetch_argument_templates", [])
                if isinstance(capability_hints.get("fetch_argument_templates"), list)
                else []
            )
            fetch_templates = [
                [maybe_text(arg) for arg in template if maybe_text(arg)]
                for template in templates
                if isinstance(template, list)
            ]
            if not fetch_templates:
                fetch_templates = [["check-config"]]
            for fetch_template in fetch_templates:
                commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        actor_role=actor_role,
                        contract_mode=contract_mode,
                        timeout_seconds=900.0,
                        retry_budget=1,
                        allow_side_effects=["network-external"],
                        skill_args=fetch_template,
                    )
                )
        elif command_kind == SKILL_LAYER_NORMALIZE:
            commands.append(
                run_skill_command(
                    run_dir=run_dir,
                    run_id=run_id,
                    round_id=round_id,
                    skill_name=skill_name,
                    actor_role=actor_role,
                    contract_mode=contract_mode,
                    timeout_seconds=900.0,
                    retry_budget=1,
                    skill_args=[] if skill_name == "normalize-fetch-execution" else ["<skill_specific_args>"],
                )
            )
    return commands


def fetch_skill_command_surfaces(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    contract_mode: str,
    actor_role: str,
    skill_names: list[str],
) -> list[dict[str, Any]]:
    surfaces: list[dict[str, Any]] = []
    for skill_name in skill_names:
        capability_hints = source_capability_hints(skill_name)
        queue_profile = source_queue_profile(skill_name)
        commands = layer_skill_commands(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            contract_mode=contract_mode,
            actor_role=actor_role,
            skill_names=[skill_name],
            command_kind=SKILL_LAYER_FETCH,
        )
        surfaces.append(
            {
                "skill_name": skill_name,
                "skill_use_card": capability_hints.get("skill_use_card", {})
                if isinstance(capability_hints.get("skill_use_card"), dict)
                else {},
                "provider_modes": capability_hints.get("provider_modes", [])
                if isinstance(capability_hints.get("provider_modes"), list)
                else [],
                "fetch_argument_templates": capability_hints.get("fetch_argument_templates", [])
                if isinstance(capability_hints.get("fetch_argument_templates"), list)
                else [],
                "source_queue_profile": queue_profile,
                "source_family_ids": queue_profile.get("source_family_ids", [])
                if isinstance(queue_profile.get("source_family_ids"), list)
                else [],
                "workflow_role": maybe_text(queue_profile.get("workflow_role")),
                "downstream_hints": queue_profile.get("downstream_hints", [])
                if isinstance(queue_profile.get("downstream_hints"), list)
                else [],
                "attempt_review_questions": queue_profile.get("attempt_review_questions", [])
                if isinstance(queue_profile.get("attempt_review_questions"), list)
                else [],
                "commands": commands,
            }
        )
    return surfaces


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
    effective_role_definitions = role_definitions_for_round(
        run_dir=run_dir,
        round_id=round_id,
        role_definitions=role_definitions,
    )
    active_round_mode = round_opening_mode(run_dir, round_id)
    for definition in effective_role_definitions:
        role = maybe_text(definition.get("role"))
        role_metadata = role_contract(role)
        grouped_skill_names = allowed_skills_by_layer(role)
        fetch_command_surfaces = fetch_skill_command_surfaces(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            contract_mode=contract_mode,
            actor_role=role,
            skill_names=grouped_skill_names.get(SKILL_LAYER_FETCH, []),
        )
        source_family_workflows = source_family_workflows_for_skills(
            grouped_skill_names.get(SKILL_LAYER_FETCH, [])
        )
        fetch_commands = [
            command
            for surface in fetch_command_surfaces
            if isinstance(surface.get("commands"), list)
            for command in surface.get("commands", [])
            if isinstance(command, str)
        ]
        normalize_commands = layer_skill_commands(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            contract_mode=contract_mode,
            actor_role=role,
            skill_names=grouped_skill_names.get(SKILL_LAYER_NORMALIZE, []),
            command_kind=SKILL_LAYER_NORMALIZE,
        )
        role_read_commands: list[str] = []
        for skill_name in definition.get("read_skills", []) if isinstance(definition.get("read_skills"), list) else []:
            if skill_name == "query-board-delta":
                role_read_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        actor_role=role,
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
                        actor_role=role,
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
        role_coordination_read_commands = coordination_read_commands(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
        )
        role_write_commands: list[str] = []
        for skill_name in definition.get("write_skills", []) if isinstance(definition.get("write_skills"), list) else []:
            if skill_name == "submit-investigation-plan":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        actor_role=role,
                        contract_mode=contract_mode,
                        skill_args=[
                            "--author-role",
                            role,
                            "--mission-ref",
                            "<mission_ref>",
                            "--rationale",
                            "<rationale>",
                            "--open-question",
                            "<open_question>",
                            "--provenance-json",
                            "{\"source\":\"<provenance_source>\"}",
                        ],
                    )
                )
            elif skill_name == "submit-investigation-scope":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        actor_role=role,
                        contract_mode=contract_mode,
                        skill_args=[
                            "--author-role",
                            role,
                            "--scope-kind",
                            "<scope_kind>",
                            "--rationale",
                            "<rationale>",
                            "--target-kind",
                            "<target_kind>",
                            "--target-id",
                            "<target_id>",
                            "--provenance-json",
                            "{\"source\":\"<provenance_source>\"}",
                        ],
                    )
                )
            elif skill_name == "submit-round-brief":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        actor_role=role,
                        contract_mode=contract_mode,
                        skill_args=[
                            "--author-role",
                            role,
                            "--round-mode",
                            "<scoping|investigation|supplemental|synthesis>",
                            "--rationale",
                            "<rationale>",
                            "--primary-focus-ref",
                            "<object_kind:object_id>",
                            "--open-question",
                            "<open_question>",
                            "--provenance-json",
                            "{\"source\":\"<provenance_source>\"}",
                        ],
                    )
                )
            elif skill_name == "submit-round-synthesis":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        actor_role=role,
                        contract_mode=contract_mode,
                        skill_args=[
                            "--author-role",
                            role,
                            "--synthesis-text",
                            "<round_stage_synthesis>",
                            "--stage-conclusion",
                            "<stage_conclusion>",
                            "--rationale",
                            "<moderator_synthesis_rationale>",
                            "--unresolved-object-ref",
                            "<object_kind:object_id>",
                            "--next-round-candidate-ref",
                            "<object_kind:object_id>",
                            "--provenance-json",
                            "{\"source\":\"<provenance_source>\"}",
                        ],
                    )
                )
            elif skill_name == "materialize-context-packet":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        actor_role=role,
                        contract_mode=contract_mode,
                        skill_args=[
                            "--author-role",
                            role,
                            "--packet-profile",
                            "<scoping|investigation|supplemental|synthesis>",
                            "--target-ref",
                            "<target_kind:target_id>",
                            "--rationale",
                            "<rationale>",
                            "--source-ref",
                            "<source_ref>",
                            "--raw-data-policy",
                            "refs-only",
                            "--provenance-json",
                            "{\"source\":\"<provenance_source>\"}",
                        ],
                    )
                )
            elif skill_name == "submit-evidence-request":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        actor_role=role,
                        contract_mode=contract_mode,
                        skill_args=[
                            "--author-role",
                            role,
                            "--question",
                            "<evidence_question>",
                            "--desired-evidence-type",
                            "<desired_evidence_type>",
                            "--rationale",
                            "<rationale>",
                            "--target-kind",
                            "<target_kind>",
                            "--target-id",
                            "<target_id>",
                            "--provenance-json",
                            "{\"source\":\"<provenance_source>\"}",
                        ],
                    )
                )
            elif skill_name == "submit-source-acquisition-proposal":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        actor_role=role,
                        contract_mode=contract_mode,
                        skill_args=[
                            "--author-role",
                            role,
                            "--source-skill",
                            "<fetch_source_skill>",
                            "--query-parameters-json",
                            "{\"query\":\"<agent_defined_query_or_params>\"}",
                            "--target-kind",
                            "<evidence-request|challenge|finding|round>",
                            "--target-id",
                            "<target_id>",
                            "--rationale",
                            "<rationale>",
                            "--declared-side-effect",
                            "network-external",
                            "--declared-side-effect",
                            "writes-artifacts",
                            "--provenance-json",
                            "{\"source\":\"<provenance_source>\"}",
                        ],
                    )
                )
            elif skill_name == "update-source-acquisition-proposal-status":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        actor_role=role,
                        contract_mode=contract_mode,
                        skill_args=[
                            "--object-id",
                            "<source_acquisition_proposal_id>",
                            "--status",
                            "<proposed|approved-for-execution|executed|withdrawn|rejected>",
                            "--actor-role",
                            role,
                            "--status-rationale",
                            "<why_this_lifecycle_update_is_being_recorded>",
                            "--evidence-ref",
                            "<receipt_or_artifact_ref>",
                            "--provenance-json",
                            "{\"source\":\"<provenance_source>\"}",
                        ],
                    )
                )
            elif skill_name == "link-source-acquisition-execution":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        actor_role=role,
                        contract_mode=contract_mode,
                        skill_args=[
                            "--object-id",
                            "<source_acquisition_proposal_id>",
                            "--actor-role",
                            role,
                            "--status",
                            "executed",
                            "--status-rationale",
                            "<why_these_execution_refs_belong_to_the_proposal>",
                            "--fetch-receipt-ref",
                            "<fetch_receipt_ref>",
                            "--normalized-signal-ref",
                            "<normalized_signal_ref>",
                            "--artifact-ref",
                            "<artifact_ref>",
                            "--provenance-json",
                            "{\"source\":\"<provenance_source>\"}",
                        ],
                    )
                )
            elif skill_name == "submit-agent-position":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        actor_role=role,
                        contract_mode=contract_mode,
                        skill_args=[
                            "--author-role",
                            role,
                            "--claim-summary",
                            "<claim_summary>",
                            "--rationale",
                            "<rationale>",
                            "--target-kind",
                            "<target_kind>",
                            "--target-id",
                            "<target_id>",
                            "--evidence-ref",
                            "<evidence_ref>",
                            "--provenance-json",
                            "{\"source\":\"<provenance_source>\"}",
                        ],
                    )
                )
            elif skill_name == "submit-challenge-disposition":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        actor_role=role,
                        contract_mode=contract_mode,
                        skill_args=[
                            "--author-role",
                            role,
                            "--target-kind",
                            "<review-comment|challenge>",
                            "--target-id",
                            "<target_object_id>",
                            "--response-to-id",
                            "<review_comment_or_challenge_id>",
                            "--disposition-status",
                            "<accepted-as-limitation|requires-followup|excluded-from-report-basis|resolved-by-followup|waived-by-challenger>",
                            "--decided-by-role",
                            role,
                            "--rationale",
                            "<rationale>",
                            "--evidence-ref",
                            "<evidence_ref>",
                            "--provenance-json",
                            "{\"source\":\"<provenance_source>\"}",
                        ],
                    )
                )
            elif skill_name == "submit-council-proposal":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        actor_role=role,
                        contract_mode=contract_mode,
                        skill_args=[
                            "--agent-role",
                            role,
                            "--proposal-kind",
                            "<proposal_kind>",
                            "--rationale",
                            "<rationale>",
                            "--confidence",
                            "<confidence_0_to_1>",
                            "--decision-source",
                            "agent-council",
                            "--target-kind",
                            "<target_kind>",
                            "--target-id",
                            "<target_id>",
                            "--evidence-ref",
                            "<evidence_ref>",
                            "--provenance-json",
                            "{\"source\":\"<provenance_source>\"}",
                        ],
                    )
                )
            elif skill_name == "submit-readiness-opinion":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        actor_role=role,
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
            elif skill_name == "post-board-note":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        actor_role=role,
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
            elif skill_name == "post-review-comment":
                role_write_commands.append(
                    kernel_command(
                        "post-review-comment",
                        "--run-dir",
                        str(run_dir),
                        "--run-id",
                        run_id,
                        "--round-id",
                        round_id,
                        "--actor-role",
                        role,
                        "--author-role",
                        role,
                        "--review-kind",
                        "<review_kind>",
                        "--comment-text",
                        "<review_comment>",
                        "--target-kind",
                        "<finding|evidence-bundle|proposal|challenge|round>",
                        "--target-id",
                        "<target_object_id>",
                        "--response-to-id",
                        "<finding_or_bundle_id>",
                        "--evidence-ref",
                        "<evidence_ref_or_evidence-bundle:id>",
                        "--provenance-json",
                        "{\"source\":\"<provenance_source>\"}",
                    )
                )
            elif skill_name == "open-challenge-ticket":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        actor_role=role,
                        contract_mode=contract_mode,
                        skill_args=[
                            "--title",
                            "<challenge_title>",
                            "--challenge-statement",
                            "<challenge_statement>",
                            "--target-hypothesis-id",
                            "<hypothesis_id>",
                            "--evidence-bundle-id",
                            "<evidence_bundle_id>",
                            "--linked-artifact-ref",
                            "<finding_or_bundle_evidence_ref>",
                            "--owner-role",
                            ROLE_CHALLENGER,
                        ],
                    )
                )
            elif skill_name == "update-hypothesis-status":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        actor_role=role,
                        contract_mode=contract_mode,
                        skill_args=[
                            "--title",
                            "<provisional_hypothesis_title>",
                            "--statement",
                            "<hypothesis_statement>",
                            "--status",
                            "active",
                            "--owner-role",
                            role,
                            "--linked-artifact-ref",
                            "finding:<finding_id>",
                            "--evidence-ref",
                            "<finding_evidence_ref>",
                        ],
                    )
                )
            elif skill_name == "open-falsification-probe":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        actor_role=role,
                        contract_mode=contract_mode,
                        skill_args=["--max-probes", "3"],
                    )
                )
            elif skill_name == "close-challenge-ticket":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        actor_role=role,
                        contract_mode=contract_mode,
                        skill_args=["--ticket-id", "<ticket_id>"],
                    )
                )
            elif skill_name == "claim-board-task":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        actor_role=role,
                        contract_mode=contract_mode,
                        skill_args=["--task-id", "<task_id>", "--claimed-by-role", ROLE_MODERATOR],
                    )
                )
            elif skill_name == "draft-narrative-report":
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        actor_role=role,
                        contract_mode=contract_mode,
                        skill_args=[
                            "--basis-round-id",
                            "<source_or_frozen_basis_round_id>",
                        ],
                    )
                )
            elif skill_name in {"validate-narrative-report", "publish-narrative-report"}:
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        actor_role=role,
                        contract_mode=contract_mode,
                    )
                )
            else:
                role_write_commands.append(
                    run_skill_command(
                        run_dir=run_dir,
                        run_id=run_id,
                        round_id=round_id,
                        skill_name=skill_name,
                        actor_role=role,
                        contract_mode=contract_mode,
                    )
                )
        if role in {
            ROLE_MODERATOR,
            ROLE_ENVIRONMENTAL_INVESTIGATOR,
            ROLE_SOCIAL_INVESTIGATOR,
            ROLE_CHALLENGER,
        }:
            role_write_commands.append(
                kernel_command(
                    "submit-finding-record",
                    "--run-dir",
                    str(run_dir),
                    "--run-id",
                    run_id,
                    "--round-id",
                    round_id,
                    "--actor-role",
                    role,
                    "--agent-role",
                    role,
                    "--finding-kind",
                    "finding",
                    "--title",
                    "<finding_title>",
                    "--summary",
                    "<finding_summary>",
                    "--rationale",
                    "<rationale>",
                    "--confidence",
                    "<confidence_0_to_1>",
                    "--target-kind",
                    "<target_kind>",
                    "--target-id",
                    "<target_id>",
                    "--basis-object-id",
                    "<basis_object_id>",
                    "--evidence-ref",
                    "<evidence_ref>",
                    "--provenance-json",
                    "{\"source\":\"<provenance_source>\"}",
                )
            )
            role_write_commands.append(
                kernel_command(
                    "post-discussion-message",
                    "--run-dir",
                    str(run_dir),
                    "--run-id",
                    run_id,
                    "--round-id",
                    round_id,
                    "--actor-role",
                    role,
                    "--author-role",
                    role,
                    "--message-text",
                    "<message_text>",
                    "--target-kind",
                    "<target_kind>",
                    "--target-id",
                    "<target_id>",
                    "--evidence-ref",
                    "<evidence_ref>",
                    "--provenance-json",
                    "{\"source\":\"<provenance_source>\"}",
                )
            )
            role_write_commands.append(
                kernel_command(
                    "submit-evidence-bundle",
                    "--run-dir",
                    str(run_dir),
                    "--run-id",
                    run_id,
                    "--round-id",
                    round_id,
                    "--actor-role",
                    role,
                    "--agent-role",
                    role,
                    "--bundle-kind",
                    "evidence-bundle",
                    "--title",
                    "<bundle_title>",
                    "--summary",
                    "<bundle_summary>",
                    "--rationale",
                    "<rationale>",
                    "--confidence",
                    "<confidence_0_to_1>",
                    "--target-kind",
                    "<target_kind>",
                    "--target-id",
                    "<target_id>",
                    "--basis-object-id",
                    "<basis_object_id>",
                    "--finding-id",
                    "<finding_id>",
                    "--evidence-ref",
                    "<evidence_ref>",
                    "--provenance-json",
                    "{\"source\":\"<provenance_source>\"}",
                )
            )
        if role in {ROLE_MODERATOR, ROLE_REPORT_EDITOR}:
            role_write_commands.append(
                kernel_command(
                    "submit-report-section-draft",
                    "--run-dir",
                    str(run_dir),
                    "--run-id",
                    run_id,
                    "--round-id",
                    round_id,
                    "--actor-role",
                    role,
                    "--agent-role",
                    role,
                    "--report-id",
                    round_id,
                    "--section-key",
                    "<section_key>",
                    "--section-title",
                    "<section_title>",
                    "--section-text",
                    "<section_text>",
                    "--basis-object-id",
                    "<basis_object_id>",
                    "--bundle-id",
                    "<bundle_id>",
                    "--finding-id",
                    "<finding_id>",
                    "--evidence-ref",
                    "<evidence_ref>",
                    "--provenance-json",
                    "{\"source\":\"<provenance_source>\"}",
                )
            )
        transition_commands: list[str] = []
        for transition_kind in (
            definition.get("transition_kinds", [])
            if isinstance(definition.get("transition_kinds"), list)
            else []
        ):
            if transition_kind == TRANSITION_KIND_FREEZE_REPORT_BASIS:
                transition_commands.append(
                    kernel_command(
                        "request-phase-transition",
                        "--run-dir",
                        str(run_dir),
                        "--run-id",
                        run_id,
                        "--round-id",
                        round_id,
                        "--transition-kind",
                        transition_kind,
                        "--rationale",
                        "<rationale>",
                        actor_role=ROLE_MODERATOR,
                    )
                )
            elif transition_kind == TRANSITION_KIND_OPEN_INVESTIGATION_ROUND:
                transition_commands.append(
                    kernel_command(
                        "request-phase-transition",
                        "--run-dir",
                        str(run_dir),
                        "--run-id",
                        run_id,
                        "--round-id",
                        round_id,
                        "--transition-kind",
                        transition_kind,
                        "--target-round-id",
                        next_round_id,
                        "--source-round-id",
                        round_id,
                        "--request-payload-json",
                        json.dumps(
                            {
                                "round_mode": "continuation",
                                "primary_focus_refs": ["<object_kind:object_id>"],
                                "continuation_basis": "moderator-selected unresolved refs",
                                "closure_reason_if_not_continuing": "<report-ready|no-actionable-path|human-paused|out-of-scope>",
                            },
                            ensure_ascii=True,
                            sort_keys=True,
                        ),
                        "--rationale",
                        "<rationale>",
                        actor_role=ROLE_MODERATOR,
                    )
                )
            elif transition_kind == TRANSITION_KIND_CLOSE_ROUND:
                transition_commands.append(
                    kernel_command(
                        "request-phase-transition",
                        "--run-dir",
                        str(run_dir),
                        "--run-id",
                        run_id,
                        "--round-id",
                        round_id,
                        "--transition-kind",
                        transition_kind,
                        "--rationale",
                        "<rationale>",
                        actor_role=ROLE_MODERATOR,
                    )
                )
            elif transition_kind == TRANSITION_KIND_OPEN_REPORT_WRITING_ROUND:
                transition_commands.append(
                    kernel_command(
                        "request-phase-transition",
                        "--run-dir",
                        str(run_dir),
                        "--run-id",
                        run_id,
                        "--round-id",
                        round_id,
                        "--transition-kind",
                        transition_kind,
                        "--target-round-id",
                        next_round_id,
                        "--source-round-id",
                        round_id,
                        "--request-payload-json",
                        json.dumps(
                            {
                                "round_mode": "report-writing",
                                "basis_round_id": round_id,
                                "reporting_basis_refs": ["<final-publication|council-decision|report-basis:object_id>"],
                                "scope": "report-editor-only narrative report production from existing council basis",
                            },
                            ensure_ascii=True,
                            sort_keys=True,
                        ),
                        "--rationale",
                        "<moderator_report_writing_rationale>",
                        actor_role=ROLE_MODERATOR,
                    )
                )
        results.append(
            {
                "role": role,
                "focus": maybe_text(definition.get("focus")),
                "role_boundary_guidance": (
                    deepcopy(definition.get("role_boundary_guidance"))
                    if isinstance(definition.get("role_boundary_guidance"), dict)
                    else {}
                ),
                "role_kind": maybe_text(role_metadata.get("role_kind")),
                "conceptual_role": maybe_text(role_metadata.get("conceptual_role")),
                "conceptual_note": maybe_text(role_metadata.get("conceptual_note")),
                "role_description": maybe_text(role_metadata.get("description")),
                "capabilities": (
                    role_metadata.get("capabilities", [])
                    if isinstance(role_metadata.get("capabilities"), list)
                    else []
                ),
                "capability_layers": capability_layers(role),
                "skill_count_by_layer": skill_count_by_layer(role),
                "skills_by_layer": grouped_skill_names,
                "fetch_commands": fetch_commands,
                "fetch_command_surfaces": fetch_command_surfaces,
                "source_family_workflows": source_family_workflows,
                "source_family_workflow_semantics": (
                    "These workflows describe optional same-family data-dependency "
                    "paths such as search-to-detail or recon-to-table-pull. They "
                    "do not rank sources, select evidence, or fix the agenda; the "
                    "role remains responsible for choosing queries, follow-up "
                    "skills, and evidence adoption."
                ),
                "skill_use_discipline": skill_use_discipline(),
                "acquisition_attempt_review_policy": {
                    "semantics": (
                        "A failed, blocked, receipt-only, executed-without-normalized-refs, "
                        "or zero-signal attempt is not by itself evidence that no source path exists."
                    ),
                    "required_owner_reflection": (
                        "Before abandoning a source family, the role should record "
                        "whether to revise query terms, change window/parameters, "
                        "use a same-family follow-up skill, switch provider, or "
                        "state an explicit source-limit rationale."
                    ),
                },
                "claim_strength_obligations": claim_strength_obligations(),
                "round_mode": maybe_text(active_round_mode),
                "entry_mode_note": (
                    "Reporting-only round: only report-editor is scheduled; investigation agents are not registered for this round."
                    if maybe_text(active_round_mode).casefold()
                    in REPORT_WRITING_ROUND_MODES
                    else ""
                ),
                "runtime_status_commands": {
                    "show_council_status": kernel_command(
                        "show-council-status",
                        "--run-dir",
                        str(run_dir),
                        "--run-id",
                        run_id,
                        "--round-id",
                        round_id,
                        "--pretty",
                    ),
                    "show_run_state": kernel_command(
                        "show-run-state",
                        "--run-dir",
                        str(run_dir),
                        "--round-id",
                        round_id,
                        "--pretty",
                    ),
                    "refresh_agent_entry_gate": kernel_command(
                        "materialize-agent-entry-gate",
                        "--run-dir",
                        str(run_dir),
                        "--run-id",
                        run_id,
                        "--round-id",
                        round_id,
                        "--pretty",
                    ),
                },
                "normalize_commands": normalize_commands,
                "read_commands": [*role_read_commands, *role_coordination_read_commands],
                "coordination_read_commands": role_coordination_read_commands,
                "analysis_commands": analysis_commands,
                "write_commands": role_write_commands,
                "transition_commands": transition_commands,
            }
        )
    return results


def default_agent_entry_recommended_skills() -> list[str]:
    return []


def default_agent_entry_operator_notes(
    *,
    status: str,
    mission: dict[str, Any],
    round_surface_payload: dict[str, Any],
    analysis: dict[str, Any],
) -> list[str]:
    notes = [
        "Agent entry now exposes role capability surfaces instead of a default investigation sequence owned by runtime kernel.",
        "Moderator remains the only role that can request phase transitions; runtime-operator approval is still required before committed state changes.",
        "Claim-strength obligations are procedural only: weak reports require explicit limitations and non-continuation rationale; strong claims require council-visible refs and challenger review path.",
    ]
    # This note is intentionally mode-agnostic here; the role surface carries the
    # exact report-writing mode once a report-only round is opened.
    notes.append("A report-writing round is a reporting-only continuation: it should register report-editor only and consume existing council/reporting basis, not reopen investigation.")
    if maybe_text(mission.get("orchestration_mode")) == "openclaw-agent":
        notes.append("Mission scaffold already marks this round as `openclaw-agent`, so the operator-visible entry chain is explicitly enabled.")
    if int(analysis.get("matching_result_set_count") or 0) > 0:
        notes.append(
            f"Analysis plane currently exposes {int(analysis.get('matching_result_set_count') or 0)} latest result sets for this round."
        )
    if maybe_text(round_surface_payload.get("state_source")) == "deliberation-plane":
        notes.append("Board state is already readable from the deliberation plane, so agent-side context does not depend on `board_summary` or `board_brief` artifacts.")
        notes.append("Structured `proposal / readiness-opinion` submissions should remain the primary council write path; board notes stay human-readable only.")
    if status == "needs-operator-review":
        notes.append("Resolve runtime health alerts or dead letters before trusting agent-guided next steps.")
    return notes[:6]


def default_agent_entry_operator_commands(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    contract_mode: str,
) -> dict[str, str]:
    if not run_id or not round_id:
        return {}
    next_round_id = default_next_round_id_builder(
        run_dir=run_dir,
        current_round_id=round_id,
    )
    return {
        "show_run_state_command": kernel_command(
            "show-run-state",
            "--run-dir",
            str(run_dir),
            "--round-id",
            round_id,
            "--pretty",
        ),
        "show_council_status_command": kernel_command(
            "show-council-status",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--pretty",
        ),
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
        "refresh_agent_entry_gate_command": kernel_command(
            "materialize-agent-entry-gate",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--pretty",
        ),
        "materialize_openclaw_agent_registration_command": kernel_command(
            "materialize-openclaw-agent-registration",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--pretty",
        ),
        "start_council_run_command_template": kernel_command(
            "start-council-run",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--mission-path",
            "<mission_path>",
            "--contract-mode",
            contract_mode,
            "--pretty",
        ),
        "read_board_delta_command": run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="query-board-delta",
            contract_mode=contract_mode,
            skill_args=["--include-closed", "--event-limit", "20"],
        ),
        "query_public_signals_command": run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="query-public-signals",
            contract_mode=contract_mode,
        ),
        "query_formal_signals_command": run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="query-formal-signals",
            contract_mode=contract_mode,
        ),
        "query_environment_signals_command": run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="query-environment-signals",
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
        "query_finding_records_command": kernel_command(
            "query-council-objects",
            "--run-dir",
            str(run_dir),
            "--object-kind",
            "finding",
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--include-contract",
            "--pretty",
        ),
        "query_discussion_messages_command": kernel_command(
            "query-council-objects",
            "--run-dir",
            str(run_dir),
            "--object-kind",
            "discussion-message",
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--include-contract",
            "--pretty",
        ),
        "query_review_comments_command": kernel_command(
            "query-council-objects",
            "--run-dir",
            str(run_dir),
            "--object-kind",
            "review-comment",
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--include-contract",
            "--pretty",
        ),
        "query_evidence_bundles_command": kernel_command(
            "query-council-objects",
            "--run-dir",
            str(run_dir),
            "--object-kind",
            "evidence-bundle",
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
        "query_investigation_plans_command": query_coordination_object_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            object_kind="investigation-plan",
        ),
        "query_subissues_command": query_coordination_object_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            object_kind="subissue",
        ),
        "query_investigation_scopes_command": query_coordination_object_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            object_kind="investigation-scope",
        ),
        "query_round_briefs_command": query_coordination_object_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            object_kind="round-brief",
        ),
        "query_round_syntheses_command": query_coordination_object_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            object_kind="round-synthesis",
        ),
        "query_evidence_requests_command": query_coordination_object_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            object_kind="evidence-request",
        ),
        "query_source_acquisition_proposals_command": query_coordination_object_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            object_kind="source-acquisition-proposal",
        ),
        "update_source_acquisition_proposal_status_command_template": run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="update-source-acquisition-proposal-status",
            actor_role=ROLE_MODERATOR,
            contract_mode=contract_mode,
            skill_args=[
                "--object-id",
                "<source_acquisition_proposal_id>",
                "--status",
                "<proposed|approved-for-execution|executed|withdrawn|rejected>",
                "--actor-role",
                ROLE_MODERATOR,
                "--status-rationale",
                "<lifecycle_update_rationale>",
                "--evidence-ref",
                "<receipt_or_artifact_ref>",
            ],
        ),
        "link_source_acquisition_execution_command_template": run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="link-source-acquisition-execution",
            actor_role=ROLE_MODERATOR,
            contract_mode=contract_mode,
            skill_args=[
                "--object-id",
                "<source_acquisition_proposal_id>",
                "--actor-role",
                ROLE_MODERATOR,
                "--status",
                "executed",
                "--status-rationale",
                "<execution_lineage_rationale>",
                "--fetch-receipt-ref",
                "<fetch_receipt_ref>",
                "--normalized-signal-ref",
                "<normalized_signal_ref>",
            ],
        ),
        "query_agent_positions_command": query_coordination_object_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            object_kind="agent-position",
        ),
        "query_context_packets_command": query_coordination_object_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            object_kind="context-packet",
        ),
        "query_report_section_drafts_command": kernel_command(
            "query-reporting-objects",
            "--run-dir",
            str(run_dir),
            "--object-kind",
            "report-section-draft",
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--include-contract",
            "--pretty",
        ),
        "query_transition_requests_command": kernel_command(
            "query-control-objects",
            "--run-dir",
            str(run_dir),
            "--object-kind",
            "transition-request",
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--pretty",
        ),
        "query_skill_approval_requests_command": kernel_command(
            "query-control-objects",
            "--run-dir",
            str(run_dir),
            "--object-kind",
            "skill-approval-request",
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--pretty",
        ),
        "query_skill_approvals_command": kernel_command(
            "query-control-objects",
            "--run-dir",
            str(run_dir),
            "--object-kind",
            "skill-approval",
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--pretty",
        ),
        "query_skill_approval_consumptions_command": kernel_command(
            "query-control-objects",
            "--run-dir",
            str(run_dir),
            "--object-kind",
            "skill-approval-consumption",
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--pretty",
        ),
        "request_optional_analysis_approval_command_template": kernel_command(
            "request-skill-approval",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--skill-name",
            "<skill_name>",
            "--requested-actor-role",
            "<requested_actor_role>",
            "--rationale",
            "<rationale>",
            "--requested-skill-arg=<skill_arg>",
            actor_role=ROLE_MODERATOR,
        ),
        "request_falsification_probe_approval_command_template": kernel_command(
            "request-skill-approval",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--skill-name",
            "open-falsification-probe",
            "--requested-actor-role",
            ROLE_CHALLENGER,
            "--requested-skill-arg",
            "--action-id=<action_id>",
            "--basis-object-id",
            "<hypothesis_or_challenge_or_bundle_id>",
            "--rationale",
            "<why_this_probe_is_needed>",
            actor_role=ROLE_CHALLENGER,
        ),
        "approve_skill_approval_command_template": kernel_command(
            "approve-skill-approval",
            "--run-dir",
            str(run_dir),
            "--request-id",
            "<request_id>",
            "--approval-reason",
            "<approval_reason>",
            actor_role="runtime-operator",
        ),
        "reject_skill_approval_command_template": kernel_command(
            "reject-skill-approval",
            "--run-dir",
            str(run_dir),
            "--request-id",
            "<request_id>",
            "--rejection-reason",
            "<rejection_reason>",
            actor_role="runtime-operator",
        ),
        "run_approved_optional_analysis_command_template": run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="<skill_name>",
            actor_role="<requested_actor_role>",
            contract_mode=contract_mode,
            skill_approval_request_id="<request_id>",
            skill_args=["<skill_specific_args>"],
        ),
        "request_report_basis_transition_command": kernel_command(
            "request-phase-transition",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--transition-kind",
            TRANSITION_KIND_FREEZE_REPORT_BASIS,
            "--rationale",
            "<rationale>",
            actor_role=ROLE_MODERATOR,
        ),
        "request_report_writing_round_command_template": kernel_command(
            "request-phase-transition",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--transition-kind",
            TRANSITION_KIND_OPEN_REPORT_WRITING_ROUND,
            "--target-round-id",
            next_round_id,
            "--source-round-id",
            round_id,
            "--request-payload-json",
            json.dumps(
                {
                    "round_mode": "report-writing",
                    "basis_round_id": round_id,
                    "reporting_basis_refs": ["<final-publication|council-decision|report-basis:object_id>"],
                    "scope": "report-editor-only narrative report production from existing council basis",
                },
                ensure_ascii=True,
                sort_keys=True,
            ),
            "--rationale",
            "<moderator_report_writing_rationale>",
            actor_role=ROLE_MODERATOR,
        ),
        "open_report_writing_round_after_approval_command_template": run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=next_round_id,
            skill_name="open-report-writing-round",
            actor_role=ROLE_MODERATOR,
            contract_mode=contract_mode,
            skill_args=[
                "--source-round-id",
                round_id,
                "--transition-request-id",
                "<approved_request_id>",
            ],
        ),
        "approve_transition_request_command_template": kernel_command(
            "approve-phase-transition",
            "--run-dir",
            str(run_dir),
            "--request-id",
            "<request_id>",
            "--approval-reason",
            "<approval_reason>",
        ),
        "reject_transition_request_command_template": kernel_command(
            "reject-phase-transition",
            "--run-dir",
            str(run_dir),
            "--request-id",
            "<request_id>",
            "--rejection-reason",
            "<rejection_reason>",
        ),
        "submit_council_proposal_command_template": run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="submit-council-proposal",
            contract_mode=contract_mode,
            skill_args=[
                "--agent-role",
                "<agent_role>",
                "--proposal-kind",
                "<proposal_kind>",
                "--rationale",
                "<rationale>",
                "--confidence",
                "<confidence_0_to_1>",
                "--target-kind",
                "<target_kind>",
                "--target-id",
                "<target_id>",
                "--response-to-id",
                "<finding_or_bundle_id>",
                "--lineage-id",
                "<finding_or_bundle_id>",
                "--evidence-ref",
                "<evidence_ref>",
                "--provenance-json",
                "{\"source\":\"<provenance_source>\"}",
            ],
        ),
        "submit_finding_record_command_template": kernel_command(
            "submit-finding-record",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--actor-role",
            "<actor_role>",
            "--agent-role",
            "<agent_role>",
            "--finding-kind",
            "finding",
            "--title",
            "<finding_title>",
            "--summary",
            "<finding_summary>",
            "--rationale",
            "<rationale>",
            "--confidence",
            "<confidence_0_to_1>",
            "--target-kind",
            "<target_kind>",
            "--target-id",
            "<target_id>",
            "--basis-object-id",
            "<basis_object_id>",
            "--evidence-ref",
            "<evidence_ref>",
            "--provenance-json",
            "{\"source\":\"<provenance_source>\"}",
        ),
        "post_discussion_message_command_template": kernel_command(
            "post-discussion-message",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--actor-role",
            "<actor_role>",
            "--author-role",
            "<author_role>",
            "--message-text",
            "<message_text>",
            "--target-kind",
            "<target_kind>",
            "--target-id",
            "<target_id>",
            "--evidence-ref",
            "<evidence_ref>",
            "--provenance-json",
            "{\"source\":\"<provenance_source>\"}",
        ),
        "post_review_comment_command_template": kernel_command(
            "post-review-comment",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--actor-role",
            "<actor_role>",
            "--author-role",
            "<challenger_or_moderator>",
            "--review-kind",
            "<review_kind>",
            "--comment-text",
            "<review_comment>",
            "--target-kind",
            "<finding|evidence-bundle|proposal|challenge|round>",
            "--target-id",
            "<target_object_id>",
            "--response-to-id",
            "<finding_or_bundle_id>",
            "--evidence-ref",
            "<evidence_ref_or_evidence-bundle:id>",
            "--provenance-json",
            "{\"source\":\"<provenance_source>\"}",
        ),
        "submit_evidence_bundle_command_template": kernel_command(
            "submit-evidence-bundle",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--actor-role",
            "<actor_role>",
            "--agent-role",
            "<agent_role>",
            "--bundle-kind",
            "evidence-bundle",
            "--title",
            "<bundle_title>",
            "--summary",
            "<bundle_summary>",
            "--rationale",
            "<rationale>",
            "--confidence",
            "<confidence_0_to_1>",
            "--target-kind",
            "<target_kind>",
            "--target-id",
            "<target_id>",
            "--basis-object-id",
            "<basis_object_id>",
            "--finding-id",
            "<finding_id>",
            "--evidence-ref",
            "<evidence_ref>",
            "--provenance-json",
            "{\"source\":\"<provenance_source>\"}",
        ),
        "update_hypothesis_from_finding_command_template": run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="update-hypothesis-status",
            contract_mode=contract_mode,
            skill_args=[
                "--title",
                "<provisional_hypothesis_title>",
                "--statement",
                "<hypothesis_statement>",
                "--status",
                "active",
                "--owner-role",
                "<agent_role>",
                "--linked-artifact-ref",
                "finding:<finding_id>",
                "--evidence-ref",
                "<finding_evidence_ref>",
            ],
        ),
        "open_challenge_on_hypothesis_or_bundle_command_template": run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="open-challenge-ticket",
            actor_role=ROLE_CHALLENGER,
            contract_mode=contract_mode,
            skill_args=[
                "--title",
                "<challenge_title>",
                "--challenge-statement",
                "<challenge_statement>",
                "--target-hypothesis-id",
                "<hypothesis_id>",
                "--evidence-bundle-id",
                "<evidence_bundle_id>",
                "--linked-artifact-ref",
                "<finding_or_bundle_evidence_ref>",
                "--owner-role",
                ROLE_CHALLENGER,
            ],
        ),
        "submit_report_section_draft_command_template": kernel_command(
            "submit-report-section-draft",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--actor-role",
            "<actor_role>",
            "--agent-role",
            "<agent_role>",
            "--report-id",
            round_id,
            "--section-key",
            "<section_key>",
            "--section-title",
            "<section_title>",
            "--section-text",
            "<section_text>",
            "--basis-object-id",
            "<basis_object_id>",
            "--bundle-id",
            "<bundle_id>",
            "--finding-id",
            "<finding_id>",
            "--evidence-ref",
            "<evidence_ref>",
            "--provenance-json",
            "{\"source\":\"<provenance_source>\"}",
        ),
        "submit_readiness_opinion_command_template": run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="submit-readiness-opinion",
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
    }


def default_agent_entry_profile() -> dict[str, Any]:
    return {
        "role_definitions": deepcopy(DEFAULT_AGENT_ENTRY_ROLE_DEFINITIONS),
        "status_evaluator": default_agent_entry_status,
        "next_round_id_builder": default_next_round_id_builder,
        "role_entry_builder": default_role_entry_points,
        "recommended_skills_builder": default_agent_entry_recommended_skills,
        "operator_notes_builder": default_agent_entry_operator_notes,
        "operator_commands_builder": default_agent_entry_operator_commands,
    }


__all__ = [
    "EntryStatusEvaluator",
    "OperatorCommandsBuilder",
    "OperatorNotesBuilder",
    "RecommendedSkillsBuilder",
    "RoleEntryBuilder",
    "coordination_read_commands",
    "default_agent_entry_operator_commands",
    "default_agent_entry_operator_notes",
    "default_agent_entry_recommended_skills",
    "default_agent_entry_status",
    "default_agent_entry_profile",
    "default_role_entry_points",
]
