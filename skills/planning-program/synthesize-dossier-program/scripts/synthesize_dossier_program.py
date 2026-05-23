#!/usr/bin/env python3
"""Synthesize a dossier-first council program from the outcome contract."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "synthesize-dossier-program"
WORKSPACE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "eco-concil-runtime").exists() and (parent / "skills").exists()
)
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.objects.council import (  # noqa: E402
    append_dynamic_investigation_object_record,
    query_council_objects,
)


EXPECTED_POSITION_ROLES = (
    "report-editor",
    "environmental-investigator",
    "social-investigator",
    "challenger",
    "moderator",
)

FORBIDDEN_SCHEDULER_FIELDS = (
    "source_family",
    "source_families",
    "source_skill",
    "source_skills",
    "query",
    "query_variants",
    "query_parameters",
    "priority_score",
    "route_ranking",
    "source_priority",
    "scheduler_queue",
    "auto_execute",
)

DOSSIER_COMPONENTS = (
    "evidence inventory",
    "structured tables",
    "timeline items",
    "episode cards",
    "representative examples",
    "denominators",
    "coverage gaps",
    "claim boundaries",
    "lineage refs",
)

THEME_REPORT_COMPONENTS = (
    "main findings",
    "timeline or episode narrative",
    "claim strength map",
    "limitations",
    "recommended final report use",
    "blocked claims",
)


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def list_items(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def dict_items(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def query_objects(
    run_dir: Path,
    *,
    object_kind: str,
    run_id: str,
    round_id: str,
    limit: int = 100,
) -> list[dict[str, Any]]:
    try:
        payload = query_council_objects(
            run_dir,
            object_kind=object_kind,
            run_id=run_id,
            round_id=round_id,
            limit=limit,
        )
    except Exception:
        return []
    return [item for item in list_items(payload.get("objects")) if isinstance(item, dict)]


def load_contract_bundle(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    contract_id: str,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    contracts = query_objects(
        run_dir,
        object_kind="report-outcome-contract",
        run_id=run_id,
        round_id=round_id,
        limit=20,
    )
    if contract_id:
        contracts = [
            item
            for item in contracts
            if maybe_text(item.get("contract_id")) == contract_id
            or maybe_text(item.get("object_id")) == contract_id
        ]
    contract = contracts[0] if contracts else {}
    blueprints = query_objects(
        run_dir,
        object_kind="report-blueprint",
        run_id=run_id,
        round_id=round_id,
        limit=20,
    )
    blueprint = blueprints[0] if blueprints else {}
    themes = query_objects(
        run_dir,
        object_kind="investigation-theme",
        run_id=run_id,
        round_id=round_id,
        limit=100,
    )
    positions = query_objects(
        run_dir,
        object_kind="agent-position",
        run_id=run_id,
        round_id=round_id,
        limit=100,
    )
    if not contract or not blueprint or not themes:
        artifact = load_json(run_dir / "reporting" / f"report_blueprint_{round_id}.json")
        if not contract and isinstance(artifact.get("report_outcome_contract"), dict):
            contract = dict_items(artifact.get("report_outcome_contract"))
        if not blueprint and isinstance(artifact.get("report_blueprint"), dict):
            blueprint = dict_items(artifact.get("report_blueprint"))
        if not themes:
            themes = [
                item
                for item in list_items(artifact.get("investigation_themes"))
                if isinstance(item, dict)
            ]
    if not contract:
        raise ValueError("No report-outcome-contract is visible for dossier program synthesis.")
    if not blueprint:
        raise ValueError("No report-blueprint is visible for dossier program synthesis.")
    return contract, blueprint, themes, positions


def role_for_theme(report_item: dict[str, Any], theme_by_id: dict[str, dict[str, Any]]) -> str:
    role = maybe_text(report_item.get("owner_role"))
    if role:
        return role
    theme = theme_by_id.get(maybe_text(report_item.get("theme_id")), {})
    return maybe_text(theme.get("owner_role")) or "moderator"


def cycle_question(report_item: dict[str, Any]) -> str:
    title = maybe_text(report_item.get("title")) or maybe_text(report_item.get("theme_report_id"))
    return f"What dossier basis and reviewed theme report are required for {title}?"


def theme_cycles(
    required_reports: list[dict[str, Any]],
    *,
    theme_by_id: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    cycles: list[dict[str, Any]] = []
    for report_item in required_reports:
        report_id = maybe_text(report_item.get("theme_report_id"))
        theme_id = maybe_text(report_item.get("theme_id"))
        owner_role = role_for_theme(report_item, theme_by_id)
        cycles.append(
            {
                "cycle_id": f"cycle-{report_id}",
                "theme_id": theme_id,
                "theme_report_id": report_id,
                "owner_role": owner_role,
                "cycle_question": cycle_question(report_item),
                "phase_sequence": [
                    "acquisition",
                    "structuring",
                    "analysis",
                    "theme-report",
                    "review",
                    "adoption",
                ],
                "required_outputs": [
                    "theme_dossier",
                    "theme_report",
                    "theme_report_review",
                    "theme_report_adoption",
                ],
                "dossier_components": list(DOSSIER_COMPONENTS),
                "theme_report_components": list(THEME_REPORT_COMPONENTS),
                "review_responsibility": "challenger",
                "adoption_responsibility": "moderator",
                "exit_criteria": [
                    "Theme dossier is readable and contains required dossier components or explicit scoped-out gaps.",
                    "Theme report cites the theme dossier and states claim strength, limits, final-report use, and blocked claims.",
                    "Challenger review is recorded before moderator adoption.",
                    "Adoption records any downgrade that must carry into final composition planning.",
                ],
            }
        )
    return cycles


def round_id_for_index(index: int, label: str) -> str:
    slug = []
    previous_dash = False
    for char in maybe_text(label).casefold():
        if char.isalnum():
            slug.append(char)
            previous_dash = False
        elif not previous_dash:
            slug.append("-")
            previous_dash = True
    return f"round-{index:03d}-" + ("".join(slug).strip("-")[:52] or "dossier-theme-cycle")


def round_for_cycle(index: int, cycle: dict[str, Any]) -> dict[str, Any]:
    report_id = maybe_text(cycle.get("theme_report_id"))
    owner_role = maybe_text(cycle.get("owner_role")) or "moderator"
    return {
        "round_id": round_id_for_index(index, report_id),
        "round_title": f"{report_id} dossier cycle",
        "round_subtitle_question": cycle_question(cycle),
        "round_mode": "dossier-theme-council",
        "round_category": "theme-dossier-cycle",
        "active_theme_ids": [maybe_text(cycle.get("theme_id"))],
        "agent_responsibility_boundaries": [
            f"{owner_role}: build the theme dossier and theme report with denominators, gaps, claim boundaries, and lineage visible.",
            f"challenger: review {report_id} for causal, public-proportion, attribution, policy-effect, and missing-denominator overreach.",
            f"moderator: adopt, adopt-with-downgrades, or block {report_id} before final composition planning.",
        ],
        "round_internal_phases": [
            "agenda-question",
            "acquisition",
            "structuring",
            "analysis",
            "theme-report",
            "challenger-review",
            "moderator-adoption",
        ],
        "expected_council_objects": [
            "theme-dossier",
            "theme-report",
            "theme-report-review",
            "theme-report-adoption",
            "theme-progress-review",
            "round-synthesis",
        ],
        "round_exit_criteria": list_items(cycle.get("exit_criteria")),
        "continuation_criteria": [
            "Carry forward only unresolved theme boundaries, denominator disputes, challenger concerns, or adoption downgrades that cannot be resolved in the cycle.",
        ],
    }


def round_sequence_from_cycles(cycles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rounds = [
        {
            "round_id": "round-001-framing-scope",
            "round_title": "Framing and dossier outcome contract",
            "round_subtitle_question": "What final report outcome contract and dossier program should govern later council work?",
            "round_mode": "framing-scope-council",
            "round_category": "planning",
            "active_theme_ids": [maybe_text(cycle.get("theme_id")) for cycle in cycles],
            "agent_responsibility_boundaries": [
                "report-editor: declare final report needs without writing final conclusions.",
                "moderator: synthesize dossier cycles, role boundaries, downgrade conditions, and review obligations.",
                "challenger: challenge unsupported final-report obligations before investigation begins.",
            ],
            "round_internal_phases": [
                "report-outcome-contract",
                "role-separated-agent-positions",
                "dossier-program-synthesis",
            ],
            "expected_council_objects": [
                "report-outcome-contract",
                "dossier-program",
                "round-brief",
            ],
            "round_exit_criteria": [
                "Report outcome contract is visible.",
                "Dossier program is visible and contains all required theme cycles.",
                "Missing role positions are recorded as framing gaps, not hidden participation.",
            ],
        }
    ]
    for index, cycle in enumerate(cycles, start=2):
        rounds.append(round_for_cycle(index, cycle))
    review_index = len(rounds) + 1
    rounds.append(
        {
            "round_id": round_id_for_index(review_index, "challenger-cross-theme-boundary-review"),
            "round_title": "Challenger cross-theme boundary review",
            "round_subtitle_question": "Which adopted or proposed theme reports need downgrades before final composition planning?",
            "round_mode": "cross-theme-boundary-review-council",
            "round_category": "cross-theme-review",
            "active_theme_ids": unique_texts([maybe_text(cycle.get("theme_id")) for cycle in cycles]),
            "agent_responsibility_boundaries": [
                "challenger: identify cross-theme causal, attribution, public-proportion, effectiveness, and denominator conflicts.",
                "moderator: ensure adopted-with-downgrades limits are carried into final composition planning.",
            ],
            "round_internal_phases": [
                "adopted-theme-report-inventory",
                "challenger-cross-theme-review",
                "moderator-boundary-synthesis",
            ],
            "expected_council_objects": [
                "theme-report-review",
                "theme-report-adoption",
                "cross-theme-boundary-review",
                "round-synthesis",
            ],
            "round_exit_criteria": [
                "Every required theme report has reviewed/adopted status or a named blocker.",
                "Cross-theme downgrade and exclusion rules are visible before composition planning.",
            ],
        }
    )
    composition_index = len(rounds) + 1
    rounds.append(
        {
            "round_id": round_id_for_index(composition_index, "final-report-composition-plan"),
            "round_title": "Final report composition plan",
            "round_subtitle_question": "How should adopted theme reports be mapped into final report sections, downgrades, and exclusions?",
            "round_mode": "final-composition-planning-council",
            "round_category": "composition-planning",
            "active_theme_ids": unique_texts([maybe_text(cycle.get("theme_id")) for cycle in cycles]),
            "agent_responsibility_boundaries": [
                "report-editor: compose only from adopted theme reports and recorded downgrade/exclusion limits.",
                "moderator: verify evidence-to-section map, claims to carry, claims to downgrade, and claims to exclude.",
                "challenger: block new final-report conclusions that bypass adopted theme reports.",
            ],
            "round_internal_phases": [
                "adopted-dossier-basis-inventory",
                "section-outline",
                "evidence-to-section-map",
                "claim-carry-downgrade-exclude-map",
                "composition-plan-adoption",
            ],
            "expected_council_objects": [
                "final-report-composition-plan",
                "round-synthesis",
            ],
            "round_exit_criteria": [
                "Composition plan names adopted theme reports and maps evidence to sections.",
                "Claims to carry, downgrade, and exclude are explicit before narrative writing.",
            ],
        }
    )
    return rounds


def round_brief_payload(
    *,
    args: argparse.Namespace,
    program_id: str,
    program_object_id: str,
    round_item: dict[str, Any],
    output_file: Path,
) -> dict[str, Any]:
    target_round_id = maybe_text(round_item.get("round_id"))
    return {
        "run_id": args.run_id,
        "round_id": target_round_id,
        "object_kind": "round-brief",
        "object_id": "round-brief-" + stable_hash(args.run_id, program_id, target_round_id)[:12],
        "author_role": maybe_text(args.author_role) or "moderator",
        "decision_source": "dossier-program-synthesis",
        "status": "draft",
        "target_kind": "round",
        "target_id": target_round_id,
        "target": {"object_kind": "round", "object_id": target_round_id},
        "rationale": "Dossier-program projected round brief; agenda context only, not acquisition routing.",
        "program_id": program_id,
        "round_title": maybe_text(round_item.get("round_title")),
        "round_subtitle_question": maybe_text(round_item.get("round_subtitle_question")),
        "round_mode": maybe_text(round_item.get("round_mode")),
        "round_category": maybe_text(round_item.get("round_category")),
        "active_theme_ids": unique_texts(list_items(round_item.get("active_theme_ids"))),
        "agent_responsibility_boundaries": unique_texts(list_items(round_item.get("agent_responsibility_boundaries"))),
        "round_internal_phases": unique_texts(list_items(round_item.get("round_internal_phases"))),
        "expected_council_objects": unique_texts(list_items(round_item.get("expected_council_objects"))),
        "round_exit_criteria": unique_texts(list_items(round_item.get("round_exit_criteria"))),
        "in_round_feedback_triggers": [
            "Theme cycle may request in-round recovery when dossier components, denominator status, or review boundaries are incomplete.",
            "Challenger concern about denominator, causal wording, public proportion, attribution, or policy evaluation boundary must be visible before adoption.",
        ],
        "supplemental_round_policy": (
            "Open a supplemental issue council only after in-round recovery is exhausted "
            "and a moderator synthesis, readiness opinion, or transition approval carries a named boundary."
        ),
        "forbidden_source_precommitments": [
            "Do not preselect provider classes, skills, route ranking, query variants, query parameters, queues, or automatic execution.",
            "Dossier phases are role-facing organization context, not a runtime phase machine.",
        ],
        "evidence_refs": [],
        "lineage": unique_texts([program_object_id, program_id, target_round_id]),
        "provenance": {
            "skill_name": SKILL_NAME,
            "decision_source": "dossier-program-synthesis",
            "artifact_path": str(output_file),
        },
    }


def round_review_row(round_item: dict[str, Any]) -> dict[str, Any]:
    return {
        "round_id": maybe_text(round_item.get("round_id")),
        "round_title": maybe_text(round_item.get("round_title")),
        "round_subtitle_question": maybe_text(round_item.get("round_subtitle_question")),
        "round_category": maybe_text(round_item.get("round_category")),
        "round_mode": maybe_text(round_item.get("round_mode")),
        "active_theme_ids": unique_texts(list_items(round_item.get("active_theme_ids"))),
        "round_internal_phases": unique_texts(list_items(round_item.get("round_internal_phases"))),
        "round_exit_criteria": unique_texts(list_items(round_item.get("round_exit_criteria"))),
    }


def human_review_packet(
    *,
    program: dict[str, Any],
    materialized_round_briefs: list[dict[str, Any]],
    missing_roles: list[str],
) -> dict[str, Any]:
    rounds = [item for item in list_items(program.get("round_sequence")) if isinstance(item, dict)]
    return {
        "schema_version": "dossier-program-human-review-v1",
        "program_id": maybe_text(program.get("program_id")),
        "object_kind": "dossier-program",
        "framing_round_id": maybe_text(program.get("round_id")),
        "mission_question": maybe_text(program.get("mission_question")),
        "adoption_status": maybe_text(program.get("adoption_status")),
        "missing_agent_position_roles": missing_roles,
        "required_theme_report_count": len(list_items(program.get("required_theme_reports"))),
        "theme_cycle_count": len(list_items(program.get("theme_cycles"))),
        "round_count": len(rounds),
        "materialized_round_brief_count": len(materialized_round_briefs),
        "planning_round": round_review_row(rounds[0]) if rounds else {},
        "theme_cycle_rounds": [
            round_review_row(item)
            for item in rounds
            if maybe_text(item.get("round_category")) == "theme-dossier-cycle"
        ],
        "cross_theme_review_rounds": [
            round_review_row(item)
            for item in rounds
            if maybe_text(item.get("round_category")) == "cross-theme-review"
        ],
        "composition_planning_round": next(
            (
                round_review_row(item)
                for item in rounds
                if maybe_text(item.get("round_category")) == "composition-planning"
            ),
            {},
        ),
        "review_status": "reviewable" if not missing_roles else "reviewable-with-framing-gaps",
        "program_boundary": {
            "not_scheduler": True,
            "not_route_plan": True,
            "not_query_plan": True,
            "not_action_queue": True,
            "final_report_consumes_adopted_theme_reports_only": True,
        },
    }


def synthesize_dossier_program(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = resolve_run_dir(args.run_dir)
    output_file = resolve_path(
        run_dir,
        args.output_path,
        f"runtime/dossier_program_{args.round_id}.json",
    )
    contract, blueprint, themes, positions = load_contract_bundle(
        run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        contract_id=maybe_text(args.contract_id),
    )
    theme_by_id = {maybe_text(theme.get("theme_id")): theme for theme in themes}
    required_reports = [
        item
        for item in list_items(contract.get("required_theme_reports"))
        if isinstance(item, dict)
    ]
    cycles = theme_cycles(required_reports, theme_by_id=theme_by_id)
    rounds = round_sequence_from_cycles(cycles)
    position_roles = {maybe_text(position.get("author_role")) for position in positions}
    missing_roles = [role for role in EXPECTED_POSITION_ROLES if role not in position_roles]
    program_id = maybe_text(args.program_id) or "dossier-program-" + stable_hash(
        args.run_id,
        args.round_id,
        contract.get("contract_id"),
    )[:12]
    boundaries = unique_texts(
        [
            boundary
            for round_item in rounds
            for boundary in list_items(round_item.get("agent_responsibility_boundaries"))
        ]
    )
    contract_ref = "report-outcome-contract:" + (
        maybe_text(contract.get("object_id")) or maybe_text(contract.get("contract_id"))
    )
    mission_question = maybe_text(contract.get("mission_question")) or maybe_text(
        (list_items(blueprint.get("report_questions")) or [""])[0]
    )
    payload = {
        "run_id": args.run_id,
        "round_id": args.round_id,
        "object_kind": "dossier-program",
        "object_id": program_id,
        "program_id": program_id,
        "author_role": maybe_text(args.author_role) or "moderator",
        "agent_role": maybe_text(args.author_role) or "moderator",
        "decision_source": "dossier-program-synthesis",
        "status": "proposed",
        "adoption_status": "proposed-for-council-use",
        "target_kind": "report-outcome-contract",
        "target_id": maybe_text(contract.get("object_id")) or maybe_text(contract.get("contract_id")),
        "target": {
            "object_kind": "report-outcome-contract",
            "object_id": maybe_text(contract.get("object_id")) or maybe_text(contract.get("contract_id")),
        },
        "rationale": "Dossier-first program synthesized from the report outcome contract and role-separated framing positions.",
        "mission_question": mission_question,
        "report_outcome_contract_ref": contract_ref,
        "program_questions": unique_texts(
            [mission_question, *[maybe_text(item.get("cycle_question")) for item in cycles]]
        ),
        "required_theme_reports": required_reports,
        "theme_cycles": cycles,
        "cross_theme_review_cycles": [
            {
                "cycle_id": "challenger-cross-theme-boundary-review",
                "reviewer_role": "challenger",
                "required_before": "final-report-composition-plan",
                "review_scope": list_items(
                    dict_items(list_items(contract.get("required_cross_theme_reviews"))[0])
                    .get("scope")
                )
                if list_items(contract.get("required_cross_theme_reviews"))
                else [
                    "causal overreach",
                    "public-proportion overreach",
                    "policy-effectiveness overreach",
                    "attribution overreach",
                ],
            }
        ],
        "agent_responsibility_boundaries": boundaries,
        "round_sequence": rounds,
        "round_exit_criteria": [
            "Every required theme cycle records dossier, theme report, challenger review, and moderator adoption or a named blocker.",
            "Adopted-with-downgrades restrictions are carried into final composition planning.",
            "Final narrative writing is blocked until final_report_composition_plan names adopted theme reports, claims to carry, claims to downgrade, and claims to exclude.",
        ],
        "downgrade_conditions": [
            "Missing dossier component downgrades the affected theme report and final section.",
            "Missing challenger review blocks adoption for final report use.",
            "Missing event_interaction_graph blocks strong interaction chapter claims.",
            "Missing policy_semantic_alignment blocks strong policy evaluation claims.",
        ],
        "supplemental_round_triggers": [
            "No reasonable in-round recovery remains for a named theme dossier or review boundary.",
            "A challenger concern, denominator dispute, adoption downgrade, or policy-alignment gap changes the issue boundary and needs moderator synthesis or transition approval.",
        ],
        "dossier_object_requirements": list(DOSSIER_COMPONENTS),
        "final_composition_requirements": [
            "central argument",
            "adopted theme reports",
            "section outline",
            "evidence-to-section map",
            "claims to carry",
            "claims to downgrade",
            "claims to exclude",
        ],
        "source_autonomy_boundary": "Investigators choose acquisition routes during their work turns or through source-acquisition-proposal / route assessment; this program does not choose routes, skills, queries, or ranking.",
        "final_report_consumption_boundary": "Final narrative report may consume adopted theme reports and the adopted final composition plan; helper-only artifacts cannot create new conclusions.",
        "forbidden_scheduler_fields": list(FORBIDDEN_SCHEDULER_FIELDS),
        "evidence_refs": [],
        "lineage": unique_texts(
            [
                args.round_id,
                maybe_text(contract.get("object_id")),
                maybe_text(blueprint.get("object_id")),
                *[maybe_text(theme.get("object_id")) for theme in themes],
                *[maybe_text(position.get("object_id")) for position in positions],
            ]
        ),
        "provenance": {
            "skill_name": SKILL_NAME,
            "decision_source": "dossier-program-synthesis",
            "artifact_path": str(output_file),
            "role_separated_council_execution": True,
        },
    }
    result = append_dynamic_investigation_object_record(
        run_dir,
        object_payload=payload,
        object_kind="dossier-program",
        artifact_path=str(output_file),
        record_locator="$.dossier_program",
    )
    stored_program = dict_items(result.get("object"))
    materialized_round_briefs: list[dict[str, Any]] = []
    for round_item in list_items(stored_program.get("round_sequence")):
        if not isinstance(round_item, dict):
            continue
        brief_payload = round_brief_payload(
            args=args,
            program_id=maybe_text(stored_program.get("program_id")),
            program_object_id=maybe_text(stored_program.get("object_id")),
            round_item=round_item,
            output_file=output_file,
        )
        brief_result = append_dynamic_investigation_object_record(
            run_dir,
            object_payload=brief_payload,
            object_kind="round-brief",
            artifact_path=str(output_file),
            record_locator="$.materialized_round_briefs",
        )
        stored_brief = dict_items(brief_result.get("object"))
        if stored_brief:
            materialized_round_briefs.append(stored_brief)
    review_packet = human_review_packet(
        program=stored_program,
        materialized_round_briefs=materialized_round_briefs,
        missing_roles=missing_roles,
    )
    wrapper = {
        "schema_version": "dossier-program-materialization-v1",
        "skill": SKILL_NAME,
        "run_id": args.run_id,
        "round_id": args.round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "report_outcome_contract": contract,
        "dossier_program": stored_program,
        "program_round_sequence": list_items(stored_program.get("round_sequence")),
        "materialized_round_briefs": materialized_round_briefs,
        "human_review_packet": review_packet,
        "missing_agent_position_roles": missing_roles,
        "program_boundaries": [
            "Dossier program decomposes final report requirements into theme cycles, not provider routes.",
            "Each theme cycle contains acquisition, structuring, analysis, theme-report, review, and adoption phases.",
            "Final report writing must wait for adopted theme reports and a final composition plan.",
        ],
        "artifact_refs": [
            {
                "artifact_path": str(output_file),
                "record_locator": "$.dossier_program",
                "artifact_ref": f"{output_file}:$.dossier_program",
            },
            {
                "artifact_path": str(output_file),
                "record_locator": "$.materialized_round_briefs",
                "artifact_ref": f"{output_file}:$.materialized_round_briefs",
            },
        ],
        "provenance": {"skill_name": SKILL_NAME, "decision_source": "dossier-program-synthesis"},
    }
    write_json(output_file, wrapper)
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": args.run_id,
            "round_id": args.round_id,
            "program_id": maybe_text(stored_program.get("program_id")),
            "required_theme_report_count": len(required_reports),
            "theme_cycle_count": len(cycles),
            "round_count": len(list_items(stored_program.get("round_sequence"))),
            "materialized_round_brief_count": len(materialized_round_briefs),
            "missing_agent_position_roles": missing_roles,
            "human_review_status": maybe_text(review_packet.get("review_status")),
            "output_path": str(output_file),
            "db_path": maybe_text(result.get("db_path")),
        },
        "receipt_id": "dossier-program-receipt-" + stable_hash(args.run_id, args.round_id, stored_program.get("program_id"))[:20],
        "artifact_refs": wrapper["artifact_refs"],
        "canonical_ids": unique_texts(
            [
                maybe_text(stored_program.get("object_id")),
                *[maybe_text(brief.get("object_id")) for brief in materialized_round_briefs],
            ]
        ),
        "warnings": [
            {
                "code": "missing-expected-agent-positions",
                "message": "Missing framing positions: " + ", ".join(missing_roles),
            }
        ]
        if missing_roles
        else [],
        "council_handoff": {
            "object_refs": [
                {
                    "object_kind": "dossier-program",
                    "object_id": maybe_text(stored_program.get("object_id")),
                },
                *[
                    {
                        "object_kind": "round-brief",
                        "object_id": maybe_text(brief.get("object_id")),
                    }
                    for brief in materialized_round_briefs
                ],
            ],
            "human_review_packet": review_packet,
            "suggested_next_skills": ["open-investigation-round"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Synthesize a dossier-first council program.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--author-role", default="moderator")
    parser.add_argument("--contract-id", default="")
    parser.add_argument("--program-id", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        payload = synthesize_dossier_program(args)
    except ValueError as exc:
        payload = {"status": "failed", "summary": {"skill": SKILL_NAME}, "message": str(exc)}
        sys.stdout.write(pretty_json(payload, args.pretty) + "\n")
        return 1
    sys.stdout.write(pretty_json(payload, args.pretty) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
