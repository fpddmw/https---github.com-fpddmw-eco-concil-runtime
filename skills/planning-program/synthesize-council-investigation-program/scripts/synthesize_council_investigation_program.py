#!/usr/bin/env python3
"""Synthesize a council investigation program from framing objects."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "synthesize-council-investigation-program"
WORKSPACE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "eco-concil-runtime").exists() and (parent / "skills").exists())
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.objects.council import append_dynamic_investigation_object_record, query_council_objects  # noqa: E402


EXPECTED_POSITION_ROLES = (
    "environmental-investigator",
    "social-investigator",
    "challenger",
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


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


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


def load_blueprint_and_themes(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    blueprint_id: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    blueprints = query_objects(
        run_dir,
        object_kind="report-blueprint",
        run_id=run_id,
        round_id=round_id,
        limit=20,
    )
    if blueprint_id:
        blueprints = [
            item
            for item in blueprints
            if maybe_text(item.get("blueprint_id")) == blueprint_id
            or maybe_text(item.get("object_id")) == blueprint_id
        ]
    blueprint = blueprints[0] if blueprints else {}
    themes = query_objects(
        run_dir,
        object_kind="investigation-theme",
        run_id=run_id,
        round_id=round_id,
        limit=100,
    )
    if not blueprint or not themes:
        artifact = load_json(run_dir / "reporting" / f"report_blueprint_{round_id}.json")
        if not blueprint and isinstance(artifact.get("report_blueprint"), dict):
            blueprint = dict_items(artifact.get("report_blueprint"))
        if not themes:
            themes = [
                item
                for item in list_items(artifact.get("investigation_themes"))
                if isinstance(item, dict)
            ]
    if not blueprint:
        raise ValueError("No report-blueprint is visible for program synthesis.")
    return blueprint, themes


def position_ref(position: dict[str, Any]) -> str:
    object_id = maybe_text(position.get("object_id"))
    return f"agent-position:{object_id}" if object_id else ""


def position_text_list(position: dict[str, Any], *field_names: str) -> list[str]:
    values: list[Any] = []
    for field_name in field_names:
        value = position.get(field_name)
        if isinstance(value, list):
            values.extend(value)
        else:
            values.append(value)
    return unique_texts(values)


def position_summary(position: dict[str, Any]) -> dict[str, Any]:
    return {
        "position_ref": position_ref(position),
        "author_role": maybe_text(position.get("author_role")),
        "status": maybe_text(position.get("status")),
        "target_id": maybe_text(position.get("target_id")),
        "position_text": maybe_text(position.get("position_text")),
        "rationale": maybe_text(position.get("rationale")),
        "boundary_notes": position_text_list(position, "boundary_notes"),
        "open_questions": position_text_list(position, "open_questions"),
        "limitations": position_text_list(position, "limitations"),
        "proposed_agenda_questions": position_text_list(
            position,
            "proposed_agenda_questions",
            "recommended_issue_questions",
            "council_agenda_questions",
        ),
    }


def theme_ref(theme: dict[str, Any]) -> str:
    return maybe_text(theme.get("theme_id")) or maybe_text(theme.get("object_id"))


def mission_question_from_blueprint(blueprint: dict[str, Any]) -> str:
    questions = [
        maybe_text(slot.get("question"))
        for slot in list_items(blueprint.get("claim_slots"))
        if isinstance(slot, dict)
    ] or [maybe_text(item) for item in list_items(blueprint.get("report_questions"))]
    return questions[0] if questions else maybe_text(blueprint.get("rationale")) or "What should this report answer?"


def agenda_question_for_theme(theme: dict[str, Any]) -> str:
    question = maybe_text(theme.get("theme_question"))
    if question.endswith(("?", "？")):
        return question
    if question:
        return question.rstrip(".") + "?"
    return f"What claim-basis boundary should the council resolve for {theme_ref(theme)}?"


def boundary_for_theme(theme: dict[str, Any]) -> str:
    owner = maybe_text(theme.get("owner_role")) or "moderator"
    theme_id = theme_ref(theme)
    claim_boundary = maybe_text(theme.get("claim_boundary"))
    if owner == "environmental-investigator":
        duty = "define fact-process claim basis, limitation, and denominator boundary"
    elif owner == "social-investigator":
        duty = "define public, media, formal, or policy-record claim basis, denominator, and limitation boundary"
    elif owner == "challenger":
        duty = "review denominator, causal, public-proportion, and policy-evaluation overreach"
    else:
        duty = "synthesize theme boundaries, unresolved concerns, and downgrade conditions"
    return f"{owner}: {duty} for {theme_id}. {claim_boundary}".strip()


def round_plan(
    *,
    round_id: str,
    title: str,
    subtitle: str,
    round_mode: str,
    round_category: str,
    active_theme_ids: list[str],
    boundaries: list[str],
    internal_phases: list[str],
) -> dict[str, Any]:
    return {
        "round_id": round_id,
        "round_title": title,
        "round_subtitle_question": subtitle if subtitle.endswith(("?", "？")) else subtitle + "?",
        "round_mode": round_mode,
        "round_category": round_category,
        "active_theme_ids": active_theme_ids,
        "agent_responsibility_boundaries": boundaries,
        "round_internal_phases": internal_phases,
        "round_exit_criteria": [
            "Council records supported, downgraded, scoped-out, or carried-forward status for each active theme boundary.",
            "Moderator synthesis or readiness opinion carries any progress-review recommendation before state transition.",
        ],
        "continuation_criteria": [
            "Open a supplemental issue council only for unresolved theme boundaries, denominator disputes, challenger concerns, or policy-lane absence after in-round recovery is exhausted.",
        ],
    }


def build_round_sequence(themes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    theme_by_id = {theme_ref(theme): theme for theme in themes if theme_ref(theme)}
    fact_ids = [
        theme_id
        for theme_id in theme_by_id
        if any(term in theme_id for term in ("fact", "official", "policy-action"))
    ]
    public_ids = [
        theme_id
        for theme_id in theme_by_id
        if any(term in theme_id for term in ("public", "semantic"))
    ]
    interaction_ids = [
        theme_id
        for theme_id in theme_by_id
        if any(term in theme_id for term in ("interaction", "timeline"))
    ]
    used = set(fact_ids + public_ids + interaction_ids)
    remaining_ids = [theme_id for theme_id in theme_by_id if theme_id not in used]
    rounds = [
        round_plan(
            round_id="round-001-framing-scope",
            title="Framing and scope council",
            subtitle="What questions must this report answer, and how should the council organize issue rounds?",
            round_mode="framing-scope-council",
            round_category="planning",
            active_theme_ids=list(theme_by_id),
            boundaries=[
                "moderator: synthesize report questions, issue-round boundaries, exit conditions, downgrade boundaries, and supplemental policy.",
                "challenger: review claim-slot overreach, denominator obligations, and unsupported policy-evaluation language.",
            ],
            internal_phases=["report-blueprint", "agent-positions", "moderator-program-synthesis"],
        )
    ]
    if fact_ids:
        rounds.append(
            round_plan(
                round_id="round-002-fact-governance",
                title="Fact and governance issue council",
                subtitle="Which fact-process and official-action boundaries can be supported or must be downgraded?",
                round_mode="issue-council",
                round_category="issue-deliberation",
                active_theme_ids=fact_ids,
                boundaries=[boundary_for_theme(theme_by_id[theme_id]) for theme_id in fact_ids],
                internal_phases=[
                    "agenda-question",
                    "agent-acquisition-turns",
                    "agent-analysis-turns",
                    "progress-review",
                    "moderator-synthesis",
                ],
            )
        )
    if public_ids:
        rounds.append(
            round_plan(
                round_id="round-003-public-semantics",
                title="Public and formal semantics issue council",
                subtitle="Which public, media, formal, or policy semantics are sample-local and denominator-bounded?",
                round_mode="issue-council",
                round_category="issue-deliberation",
                active_theme_ids=public_ids,
                boundaries=[boundary_for_theme(theme_by_id[theme_id]) for theme_id in public_ids],
                internal_phases=[
                    "agenda-question",
                    "agent-acquisition-turns",
                    "agent-analysis-turns",
                    "progress-review",
                    "moderator-synthesis",
                ],
            )
        )
    if interaction_ids or remaining_ids:
        active_ids = [*interaction_ids, *remaining_ids]
        rounds.append(
            round_plan(
                round_id="round-004-interaction-policy-boundary",
                title="Interaction and policy-basis issue council",
                subtitle="What interaction chronology and policy-evaluation basis can the report carry without causal overreach?",
                round_mode="issue-council",
                round_category="issue-deliberation",
                active_theme_ids=active_ids,
                boundaries=[boundary_for_theme(theme_by_id[theme_id]) for theme_id in active_ids],
                internal_phases=[
                    "agenda-question",
                    "agent-analysis-turns",
                    "progress-review",
                    "moderator-synthesis",
                ],
            )
        )
    rounds.append(
        round_plan(
            round_id=f"round-{len(rounds) + 1:03d}-report-writing",
            title="Report writing council handoff",
            subtitle="Which council-carried materials can enter the report, and which claims must remain limitations?",
            round_mode="report-writing",
            round_category="reporting",
            active_theme_ids=list(theme_by_id),
            boundaries=[
                "report-editor: organize only council-carried basis, section briefs, frozen basis, and visible limitations into report prose.",
                "challenger: review strong claims, policy evaluation, public proportions, causal wording, and attribution language before report use.",
                "moderator: ensure reporting handoff preserves unresolved boundaries, downgrade requirements, and transition approvals.",
            ],
            internal_phases=[
                "agent-section-briefs",
                "reporting-handoff",
                "draft-validation",
                "publication",
            ],
        )
    )
    return rounds


def round_brief_payload_from_program_round(
    *,
    args: argparse.Namespace,
    program_id: str,
    program_object_id: str,
    round_item: dict[str, Any],
    output_file: Path,
) -> dict[str, Any]:
    target_round_id = maybe_text(round_item.get("round_id"))
    brief_id = "round-brief-" + stable_hash(args.run_id, program_id, target_round_id)[:12]
    is_reporting_round = maybe_text(round_item.get("round_category")) == "reporting"
    expected_objects = (
        [
            "agent-section-brief",
            "reporting-handoff",
            "narrative-report-draft",
            "narrative-report-validation",
            "narrative-report",
        ]
        if is_reporting_round
        else [
            "theme-evidence-boundary-plan",
            "source-acquisition-proposal",
            "evidence-route-assessment",
            "finding",
            "evidence-bundle",
            "theme-progress-review",
            "round-synthesis",
        ]
    )
    return {
        "run_id": args.run_id,
        "round_id": target_round_id,
        "object_kind": "round-brief",
        "object_id": brief_id,
        "brief_id": brief_id,
        "author_role": maybe_text(args.author_role) or "moderator",
        "decision_source": "moderator-program-synthesis",
        "status": "draft",
        "target_kind": "round",
        "target_id": target_round_id,
        "target_round_id": target_round_id,
        "planning_round_id": args.round_id,
        "target": {
            "object_kind": "round",
            "object_id": target_round_id,
        },
        "rationale": (
            "Program-projected round brief. It carries agenda questions, active "
            "themes, responsibility boundaries, exit criteria, and supplement "
            "policy only; it does not choose acquisition routes."
        ),
        "program_id": program_id,
        "round_title": maybe_text(round_item.get("round_title")),
        "round_subtitle_question": maybe_text(round_item.get("round_subtitle_question")),
        "round_mode": maybe_text(round_item.get("round_mode")),
        "round_category": maybe_text(round_item.get("round_category")),
        "active_theme_ids": unique_texts(list_items(round_item.get("active_theme_ids"))),
        "agent_responsibility_boundaries": unique_texts(
            list_items(round_item.get("agent_responsibility_boundaries"))
        ),
        "round_internal_phases": unique_texts(list_items(round_item.get("round_internal_phases"))),
        "expected_council_objects": expected_objects,
        "round_exit_criteria": unique_texts(list_items(round_item.get("round_exit_criteria"))),
        "in_round_feedback_triggers": [
            "Low or zero result acquisition attempt requires source-owner recovery reflection before changing claim strength.",
            "Analysis turn can request in-round recovery when basis refs, denominator status, or policy lane coverage are not yet council-carried.",
            "Challenger concern about denominator, causal wording, public proportion, or policy evaluation boundary must be visible before report use.",
        ],
        "supplemental_round_policy": (
            "Supplemental issue council is only appropriate after in-round "
            "recovery is exhausted and a moderator synthesis, readiness opinion, "
            "report-basis gate, or transition approval carries the need."
        ),
        "forbidden_source_precommitments": [
            "Do not preselect provider families, skills, query variants, query parameters, route ranking, source priority, scheduler queue, or automatic execution.",
            "Acquisition and analysis are agent work turns inside the issue council round, not automatic runtime phases.",
        ],
        "evidence_refs": [],
        "lineage": unique_texts([program_object_id, program_id, target_round_id]),
        "provenance": {
            "skill_name": SKILL_NAME,
            "decision_source": "moderator-program-synthesis",
            "artifact_path": str(output_file),
        },
    }


def synthesize_program(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = resolve_run_dir(args.run_dir)
    output_file = resolve_path(
        run_dir,
        args.output_path,
        f"runtime/council_investigation_program_{args.round_id}.json",
    )
    blueprint, themes = load_blueprint_and_themes(
        run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        blueprint_id=maybe_text(args.blueprint_id),
    )
    positions = query_objects(
        run_dir,
        object_kind="agent-position",
        run_id=args.run_id,
        round_id=args.round_id,
        limit=100,
    )
    position_summaries = [
        summary
        for summary in [position_summary(position) for position in positions]
        if maybe_text(summary.get("position_ref"))
    ]
    position_roles = {maybe_text(item.get("author_role")) for item in positions}
    missing_roles = [role for role in EXPECTED_POSITION_ROLES if role not in position_roles]
    program_id = maybe_text(args.program_id) or "council-program-" + stable_hash(args.run_id, args.round_id, blueprint.get("object_id"))[:12]
    theme_threads = [
        {
            "theme_id": theme_ref(theme),
            "theme_question": agenda_question_for_theme(theme),
            "claim_slots_supported": list_items(theme.get("claim_slots_supported")),
            "claim_basis_boundary": maybe_text(theme.get("claim_boundary")),
            "owner_role": maybe_text(theme.get("owner_role")),
        }
        for theme in themes
        if theme_ref(theme)
    ]
    round_sequence = build_round_sequence(themes)
    agenda_questions = unique_texts(
        [
            agenda_question_for_theme(theme)
            for theme in themes
            if theme_ref(theme)
        ]
        + [
            question
            for summary in position_summaries
            for question in list_items(summary.get("proposed_agenda_questions"))
        ]
    )
    boundaries = unique_texts(
        [
            boundary
            for round_item in round_sequence
            for boundary in list_items(round_item.get("agent_responsibility_boundaries"))
        ]
    )
    blueprint_ref = "report-blueprint:" + (
        maybe_text(blueprint.get("object_id")) or maybe_text(blueprint.get("blueprint_id"))
    )
    payload = {
        "run_id": args.run_id,
        "round_id": args.round_id,
        "object_kind": "council-investigation-program",
        "object_id": program_id,
        "program_id": program_id,
        "author_role": maybe_text(args.author_role) or "moderator",
        "agent_role": maybe_text(args.author_role) or "moderator",
        "decision_source": "moderator-program-synthesis",
        "status": "proposed",
        "adoption_status": "proposed-for-council-use",
        "target_kind": "report-blueprint",
        "target_id": maybe_text(blueprint.get("object_id")) or maybe_text(blueprint.get("blueprint_id")),
        "target": {
            "object_kind": "report-blueprint",
            "object_id": maybe_text(blueprint.get("object_id")) or maybe_text(blueprint.get("blueprint_id")),
        },
        "rationale": "Program-aware council flow synthesized from report blueprint and agent positions; no acquisition route is selected.",
        "mission_question": mission_question_from_blueprint(blueprint),
        "report_blueprint_ref": blueprint_ref,
        "agent_position_refs": unique_texts([position_ref(position) for position in positions]),
        "agent_position_summaries": position_summaries,
        "framing_position_roles": unique_texts([summary.get("author_role") for summary in position_summaries]),
        "missing_agent_position_roles": missing_roles,
        "program_questions": unique_texts(list_items(blueprint.get("report_questions")) or [mission_question_from_blueprint(blueprint)]),
        "theme_threads": theme_threads,
        "council_agenda_questions": agenda_questions,
        "agent_responsibility_boundaries": boundaries,
        "round_sequence": round_sequence,
        "round_internal_phase_model": [
            "round_internal_phases are descriptive organization hints only",
            "acquisition and analysis are agent work turns inside issue council rounds",
            "progress review is advisory until carried by council object, moderator synthesis, readiness opinion, report-basis gate, or transition approval",
        ],
        "round_exit_criteria": [
            "Each active theme has a support, downgrade, scope-out, or named continuation disposition recorded by council-facing objects.",
            "Strong claims, public proportions, causal wording, attribution wording, and policy evaluation boundaries have challenger-visible review before report use.",
        ],
        "downgrade_conditions": [
            "Missing denominator basis downgrades public or formal semantic claims to examples, cues, limitations, or future work.",
            "Missing official action or governance basis blocks policy effectiveness conclusions.",
            "Missing relation or attribution basis downgrades causal, transport, source-origin, or public-response wording.",
        ],
        "supplemental_round_triggers": [
            "No reasonable in-round recovery remains for a named theme responsibility boundary.",
            "A challenger concern, denominator dispute, policy-lane absence, or source-limit dispute changes the issue boundary and needs moderator synthesis or transition approval.",
        ],
        "source_autonomy_boundary": "Investigators choose acquisition routes during their work turns or through source-acquisition-proposal / route assessment; this program does not choose sources, skills, routes, or queries.",
        "policy_evaluation_boundary": "policy_evaluation_basis is a report synthesis boundary from carried fact, official-action, public/formal semantic, interaction, and challenger limitation basis; it is not an acquisition lane.",
        "forbidden_scheduler_fields": list(FORBIDDEN_SCHEDULER_FIELDS),
        "evidence_refs": [],
        "lineage": unique_texts(
            [
                args.round_id,
                maybe_text(blueprint.get("object_id")),
                *[theme_ref(theme) for theme in themes],
                *[maybe_text(position.get("object_id")) for position in positions],
            ]
        ),
        "provenance": {
            "skill_name": SKILL_NAME,
            "decision_source": "moderator-program-synthesis",
            "artifact_path": str(output_file),
        },
    }
    result = append_dynamic_investigation_object_record(
        run_dir,
        object_payload=payload,
        object_kind="council-investigation-program",
        artifact_path=str(output_file),
        record_locator="$.council_investigation_program",
    )
    stored_program = dict_items(result.get("object"))
    materialized_round_briefs: list[dict[str, Any]] = []
    for round_item in list_items(stored_program.get("round_sequence")):
        if not isinstance(round_item, dict):
            continue
        brief_payload = round_brief_payload_from_program_round(
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
    wrapper = {
        "schema_version": "council-investigation-program-materialization-v1",
        "skill": SKILL_NAME,
        "run_id": args.run_id,
        "round_id": args.round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "council_investigation_program": stored_program,
        "program_round_sequence": list_items(stored_program.get("round_sequence")),
        "materialized_round_briefs": materialized_round_briefs,
        "missing_agent_position_roles": missing_roles,
        "program_boundaries": [
            "Program questions become council agenda questions, not fixed report sections or task queues.",
            "Agent responsibility boundaries are round responsibilities, not source/query/skill sequences.",
            "Round internal phases are descriptive organization hints, not a runtime state machine.",
        ],
        "artifact_refs": [
            {
                "artifact_path": str(output_file),
                "record_locator": "$.council_investigation_program",
                "artifact_ref": f"{output_file}:$.council_investigation_program",
            },
            {
                "artifact_path": str(output_file),
                "record_locator": "$.materialized_round_briefs",
                "artifact_ref": f"{output_file}:$.materialized_round_briefs",
            }
        ],
        "provenance": {"skill_name": SKILL_NAME, "decision_source": "moderator-program-synthesis"},
    }
    write_json(output_file, wrapper)
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": args.run_id,
            "round_id": args.round_id,
            "program_id": maybe_text(stored_program.get("program_id")),
            "round_count": len(list_items(stored_program.get("round_sequence"))),
            "materialized_round_brief_count": len(materialized_round_briefs),
            "agenda_question_count": len(list_items(stored_program.get("council_agenda_questions"))),
            "missing_agent_position_roles": missing_roles,
            "output_path": str(output_file),
            "db_path": maybe_text(result.get("db_path")),
        },
        "receipt_id": "council-program-receipt-" + stable_hash(args.run_id, args.round_id, stored_program.get("program_id"))[:20],
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
                    "object_kind": "council-investigation-program",
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
            "suggested_next_skills": ["submit-round-brief", "open-investigation-round"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Synthesize a program-aware council investigation flow.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--author-role", default="moderator")
    parser.add_argument("--blueprint-id", default="")
    parser.add_argument("--program-id", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        payload = synthesize_program(args)
    except ValueError as exc:
        payload = {"status": "failed", "summary": {"skill": SKILL_NAME}, "message": str(exc)}
        sys.stdout.write(pretty_json(payload, args.pretty) + "\n")
        return 1
    sys.stdout.write(pretty_json(payload, args.pretty) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
