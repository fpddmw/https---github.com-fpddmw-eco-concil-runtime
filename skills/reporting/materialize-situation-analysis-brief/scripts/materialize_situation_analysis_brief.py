#!/usr/bin/env python3
"""Materialize a situation-analysis brief from carried reporting basis."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "materialize-situation-analysis-brief"
WORKSPACE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "eco-concil-runtime").exists() and (parent / "skills").exists()
)
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.objects.council import query_council_objects  # noqa: E402
from eco_council_runtime.reporting_objects import (  # noqa: E402
    query_reporting_objects,
    store_situation_analysis_brief_record,
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


def pretty_json(payload: Any, pretty: bool) -> str:
    if pretty:
        return json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True)
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


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
    path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def unique_values(values: list[Any]) -> list[Any]:
    seen: set[str] = set()
    results: list[Any] = []
    for value in values:
        try:
            key = json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        except TypeError:
            key = maybe_text(value)
        if not key or key in seen:
            continue
        seen.add(key)
        results.append(value)
    return results


def unique_texts(values: list[Any]) -> list[str]:
    return [maybe_text(value) for value in unique_values(values) if maybe_text(value)]


def query_reporting(
    run_dir: Path,
    *,
    object_kind: str,
    run_id: str,
    round_id: str,
    limit: int = 100,
) -> list[dict[str, Any]]:
    try:
        payload = query_reporting_objects(
            run_dir,
            object_kind=object_kind,
            run_id=run_id,
            round_id=round_id,
            limit=limit,
        )
    except Exception:
        return []
    return [item for item in list_items(payload.get("objects")) if isinstance(item, dict)]


def query_council(
    run_dir: Path,
    *,
    object_kind: str,
    run_id: str,
    round_id: str = "",
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


def load_latest_program(
    run_dir: Path,
    *,
    run_id: str,
    program_id: str,
) -> dict[str, Any]:
    programs = query_council(
        run_dir,
        object_kind="council-investigation-program",
        run_id=run_id,
        limit=50,
    )
    for program in programs:
        if not program_id:
            return program
        if maybe_text(program.get("program_id")) == program_id or maybe_text(program.get("object_id")) == program_id:
            return program
    return {}


def load_theme_progress_reviews(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
) -> list[dict[str, Any]]:
    rows = query_council(
        run_dir,
        object_kind="theme-progress-review",
        run_id=run_id,
        round_id=round_id,
        limit=100,
    )
    artifact = load_json(run_dir / "analytics" / f"theme_sufficiency_review_{round_id}.json")
    rows.extend(
        item
        for item in list_items(artifact.get("theme_progress_reviews"))
        if isinstance(item, dict)
    )
    return [
        item
        for item in unique_values(rows)
        if isinstance(item, dict)
    ]


def load_interaction_context(run_dir: Path, basis_round_id: str) -> dict[str, Any]:
    path = run_dir / "analytics" / f"fact_policy_public_interaction_timeline_{basis_round_id}.json"
    payload = load_json(path)
    return {
        "path": str(path),
        "interaction_nodes": [
            item
            for item in list_items(payload.get("interaction_nodes"))
            if isinstance(item, dict)
        ],
        "lane_episode_cards": [
            item
            for item in list_items(payload.get("lane_episode_cards"))
            if isinstance(item, dict)
        ],
    }


def load_report_basis_refs(run_dir: Path, basis_round_id: str) -> dict[str, Any]:
    candidates = [
        run_dir / "report_basis" / f"report_basis_freeze_{basis_round_id}.json",
        run_dir / "report_basis" / f"report_basis_{basis_round_id}.json",
    ]
    for path in candidates:
        payload = load_json(path)
        if payload:
            return {
                "path": str(path),
                "selected_basis_object_ids": list_items(payload.get("selected_basis_object_ids")),
                "selected_evidence_refs": list_items(payload.get("selected_evidence_refs")),
            }
    return {"path": "", "selected_basis_object_ids": [], "selected_evidence_refs": []}


def mission_question(args: argparse.Namespace, run_dir: Path, program: dict[str, Any]) -> str:
    explicit = maybe_text(args.mission_text)
    if explicit:
        return explicit
    if maybe_text(program.get("mission_question")):
        return maybe_text(program.get("mission_question"))
    mission = load_json(run_dir / "mission.json")
    return (
        maybe_text(mission.get("request_text"))
        or maybe_text(mission.get("objective"))
        or maybe_text(mission.get("topic"))
        or "What situation analysis can the current carried basis support?"
    )


def text_contains_any(text: str, terms: tuple[str, ...]) -> bool:
    lowered = maybe_text(text).casefold()
    return any(term in lowered for term in terms)


def brief_texts(brief: dict[str, Any]) -> list[str]:
    return unique_texts(
        [
            *list_items(brief.get("main_claims")),
            *list_items(brief.get("limitations")),
            maybe_text(brief.get("section_role")),
            maybe_text(brief.get("recommended_report_use")),
        ]
    )


def chain_from_briefs(
    briefs: list[dict[str, Any]],
    *,
    terms: tuple[str, ...],
    fallback: str,
    limit: int = 5,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for brief in briefs:
        haystack = " ".join(
            [
                maybe_text(brief.get("agent_role")),
                maybe_text(brief.get("section_key")),
                maybe_text(brief.get("section_role")),
                " ".join(brief_texts(brief)),
            ]
        )
        if terms and not text_contains_any(haystack, terms):
            continue
        texts = brief_texts(brief)
        if not texts:
            continue
        rows.append(
            {
                "section_brief_id": maybe_text(brief.get("brief_id")),
                "agent_role": maybe_text(brief.get("agent_role")),
                "summary": texts[0],
                "limitations": list_items(brief.get("limitations"))[:3],
                "evidence_refs": list_items(brief.get("evidence_refs"))[:8],
            }
        )
        if len(rows) >= limit:
            break
    if rows:
        return rows
    return [{"summary": fallback, "limitations": [fallback], "evidence_refs": []}]


def central_judgement(
    *,
    section_briefs: list[dict[str, Any]],
    progress_reviews: list[dict[str, Any]],
) -> str:
    supported = [
        claim
        for brief in section_briefs
        if maybe_text(brief.get("claim_strength")) not in {
            "insufficient-basis-downgrade-required",
            "brief-only-no-sufficiency-review",
        }
        for claim in list_items(brief.get("main_claims"))
        if maybe_text(claim)
    ]
    if supported:
        return "Current carried section briefs support this bounded report line: " + maybe_text(supported[0])
    dispositions = unique_texts(
        [review.get("recommended_disposition") for review in progress_reviews]
    )
    if dispositions:
        return (
            "Current carried progress review requires bounded reporting with "
            "visible disposition(s): "
            + ", ".join(dispositions)
            + "."
        )
    return "Current carried basis supports only a limitation-aware situation analysis; no new fact is added by this brief."


def build_brief(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = resolve_run_dir(args.run_dir)
    basis_round_id = maybe_text(args.basis_round_id) or args.round_id
    output_file = resolve_path(
        run_dir,
        args.output_path,
        f"reporting/situation_analysis_brief_{args.round_id}.json",
    )
    section_briefs = query_reporting(
        run_dir,
        object_kind="agent-section-brief",
        run_id=args.run_id,
        round_id=basis_round_id,
        limit=100,
    )
    progress_reviews = load_theme_progress_reviews(
        run_dir,
        run_id=args.run_id,
        round_id=basis_round_id,
    )
    program_id = maybe_text(args.program_id) or maybe_text(
        next(
            (
                review.get("program_id")
                for review in progress_reviews
                if maybe_text(review.get("program_id"))
            ),
            "",
        )
    )
    program = load_latest_program(run_dir, run_id=args.run_id, program_id=program_id)
    program_id = program_id or maybe_text(program.get("program_id")) or "program-not-linked"
    interaction = load_interaction_context(run_dir, basis_round_id)
    frozen = load_report_basis_refs(run_dir, basis_round_id)
    challenger_briefs = [
        brief
        for brief in section_briefs
        if maybe_text(brief.get("agent_role")) == "challenger"
    ]
    challenger_reviews = query_council(
        run_dir,
        object_kind="review-comment",
        run_id=args.run_id,
        round_id=basis_round_id,
        limit=50,
    )
    evidence_refs = unique_values(
        [
            *list_items(frozen.get("selected_evidence_refs")),
            *[
                ref
                for brief in section_briefs
                for ref in list_items(brief.get("evidence_refs"))
            ],
            *[
                ref
                for review in progress_reviews
                for ref in list_items(review.get("evidence_refs"))
            ],
        ]
    )
    section_brief_refs = unique_texts(
        [
            f"agent-section-brief:{brief.get('brief_id')}"
            for brief in section_briefs
            if maybe_text(brief.get("brief_id"))
        ]
    )
    basis_ids = unique_texts(
        [
            *list_items(frozen.get("selected_basis_object_ids")),
            *[
                basis_id
                for brief in section_briefs
                for basis_id in list_items(brief.get("basis_object_ids"))
            ],
        ]
    )
    unresolved = unique_values(
        [
            *[
                {
                    "active_theme_id": maybe_text(review.get("active_theme_id")),
                    "recommended_disposition": maybe_text(review.get("recommended_disposition")),
                    "limits": list_items(review.get("coverage_or_policy_lane_limits"))[:4],
                }
                for review in progress_reviews
                if maybe_text(review.get("recommended_disposition"))
                not in {"satisfied-for-current-claim-strength", ""}
            ],
            *[
                {
                    "section_brief_id": maybe_text(brief.get("brief_id")),
                    "limitations": list_items(brief.get("limitations"))[:4],
                }
                for brief in section_briefs
                if maybe_text(brief.get("claim_strength")) == "insufficient-basis-downgrade-required"
            ],
        ]
    )
    event_stage_map = [
        {
            "stage_id": "carried-basis",
            "stage_label": "Carried basis before report writing",
            "basis": "agent section briefs, frozen basis refs, council objects, accepted review, and challenger boundaries only",
        }
    ]
    if interaction["interaction_nodes"]:
        event_stage_map.append(
            {
                "stage_id": "interaction-context",
                "stage_label": "Fact/official/public co-visibility context",
                "interaction_node_count": len(interaction["interaction_nodes"]),
                "lane_episode_card_count": len(interaction["lane_episode_cards"]),
            }
        )
    policy_basis = chain_from_briefs(
        section_briefs,
        terms=("policy", "official", "governance", "evaluation"),
        fallback="No official-action or policy-evaluation basis has been carried; report policy evaluation only as limitation or follow-up dimension.",
        limit=6,
    )
    brief = {
        "brief_id": "situation-analysis-brief-"
        + stable_hash(args.run_id, args.round_id, basis_round_id, program_id, section_brief_refs)[:12],
        "run_id": args.run_id,
        "round_id": args.round_id,
        "basis_round_id": basis_round_id,
        "generated_at_utc": utc_now_iso(),
        "decision_source": "report-editor-synthesis",
        "status": "materialized",
        "object_kind": "situation-analysis-brief",
        "program_id": program_id,
        "mission_answerable_question": mission_question(args, run_dir, program),
        "central_bounded_judgement": central_judgement(
            section_briefs=section_briefs,
            progress_reviews=progress_reviews,
        ),
        "event_stage_map": event_stage_map,
        "fact_process_chain": chain_from_briefs(
            section_briefs,
            terms=("fact", "environment", "event", "process", "observation"),
            fallback="No fact-process section brief has been carried; report must state this as a fact-basis limitation.",
        ),
        "official_action_chain": chain_from_briefs(
            section_briefs,
            terms=("official", "policy", "governance", "formal"),
            fallback="No official-action section brief has been carried; official action claims must be omitted or downgraded.",
        ),
        "public_semantic_chain": chain_from_briefs(
            section_briefs,
            terms=("public", "semantic", "media", "discourse", "formal"),
            fallback="No public/media/formal semantic section brief has been carried; public meaning claims must be omitted or downgraded.",
        ),
        "policy_semantic_chain": policy_basis,
        "interaction_claims": [
            {
                "node_id": maybe_text(node.get("node_id")),
                "summary": maybe_text(node.get("summary"))
                or "Interaction node requires report-editor wording as descriptive co-visibility only.",
                "fact_or_policy_evidence_refs": list_items(node.get("fact_or_policy_evidence_refs")),
                "public_or_media_evidence_refs": list_items(node.get("public_or_media_evidence_refs")),
                "boundary": "Same-window visibility is not causality, policy effect, or public response attribution.",
            }
            for node in interaction["interaction_nodes"][:8]
        ]
        or [
            {
                "summary": "No carried interaction timeline node is visible; keep fact, official, and public semantic lanes separate.",
                "boundary": "Do not invent interaction claims.",
            }
        ],
        "policy_evaluation_basis": policy_basis,
        "downgraded_claims": unresolved
        or [
            {
                "summary": "No downgraded claim need was materialized; report-editor must still preserve blocked wording and challenger boundaries.",
            }
        ],
        "unresolved_claim_needs": unresolved,
        "section_brief_refs": section_brief_refs,
        "basis_object_ids": basis_ids,
        "evidence_refs": evidence_refs,
        "challenger_boundary_refs": unique_texts(
            [
                f"agent-section-brief:{brief.get('brief_id')}"
                for brief in challenger_briefs
                if maybe_text(brief.get("brief_id"))
            ]
            + [
                f"review-comment:{review.get('comment_id') or review.get('object_id')}"
                for review in challenger_reviews
                if maybe_text(review.get("comment_id") or review.get("object_id"))
            ]
        ),
        "recommended_report_spine": [
            "Start by directly answering the mission question with the central bounded judgement.",
            "Then narrate the event or governance stages using only carried fact-process and official-action basis.",
            "Connect public/media/formal semantics to the fact and official lanes as bounded meanings, not representative public opinion or causality.",
            "Close policy evaluation as a boundary of preparedness, communication, coverage, participation, or follow-up needs only where carried basis supports it.",
            "List downgraded or unresolved claims before validation.",
        ],
        "forbidden_writing_upgrades": [
            "Do not add facts absent from section briefs, frozen basis, council objects, accepted review, or challenger boundaries.",
            "Do not turn source-family-local public semantics into representative public opinion.",
            "Do not treat GDELT media/document tone as public sentiment.",
            "Do not turn public source narratives into physical source attribution.",
            "Do not write policy effectiveness, causality, responsibility, or adequacy beyond carried basis and challenger-visible limits.",
            "Do not present this situation-analysis brief as a new data source, investigation round, runtime gate, or report正文.",
        ],
        "lineage": unique_texts(
            [
                program_id,
                basis_round_id,
                *section_brief_refs,
                *basis_ids,
                *[
                    maybe_text(review.get("review_id"))
                    for review in progress_reviews
                    if maybe_text(review.get("review_id"))
                ],
            ]
        ),
        "provenance": {
            "skill_name": SKILL_NAME,
            "decision_source": "report-editor-synthesis",
            "artifact_path": str(output_file),
            "not_new_data_source": True,
            "not_runtime_gate": True,
        },
    }
    store_result = store_situation_analysis_brief_record(
        run_dir,
        brief_payload=brief,
        artifact_path=str(output_file),
    )
    stored_brief = dict_items(store_result.get("brief"))
    wrapper = {
        "schema_version": "situation-analysis-brief-materialization-v1",
        "skill": SKILL_NAME,
        "run_id": args.run_id,
        "round_id": args.round_id,
        "basis_round_id": basis_round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "brief": stored_brief,
        "section_brief_count": len(section_briefs),
        "theme_progress_review_count": len(progress_reviews),
        "db_path": maybe_text(store_result.get("db_path")),
        "artifact_refs": [
            {
                "artifact_path": str(output_file),
                "record_locator": "$.brief",
                "artifact_ref": f"{output_file}:$.brief",
            }
        ],
        "provenance": {
            "skill_name": SKILL_NAME,
            "decision_source": "report-editor-synthesis",
        },
    }
    write_json(output_file, wrapper)
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": args.run_id,
            "round_id": args.round_id,
            "basis_round_id": basis_round_id,
            "brief_id": maybe_text(stored_brief.get("brief_id")),
            "program_id": maybe_text(stored_brief.get("program_id")),
            "section_brief_count": len(section_briefs),
            "theme_progress_review_count": len(progress_reviews),
            "output_path": str(output_file),
            "db_path": maybe_text(store_result.get("db_path")),
        },
        "receipt_id": "situation-analysis-brief-receipt-"
        + stable_hash(args.run_id, args.round_id, stored_brief.get("brief_id"))[:20],
        "artifact_refs": wrapper["artifact_refs"],
        "canonical_ids": [maybe_text(stored_brief.get("brief_id"))],
        "warnings": [],
        "reporting_handoff": {
            "brief_ref": {
                "object_kind": "situation-analysis-brief",
                "object_id": maybe_text(stored_brief.get("brief_id")),
            },
            "suggested_next_skills": [
                "materialize-reporting-handoff",
                "draft-narrative-report",
                "validate-narrative-report",
            ],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize a situation-analysis brief.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--basis-round-id", default="")
    parser.add_argument("--program-id", default="")
    parser.add_argument("--mission-text", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        payload = build_brief(args)
    except ValueError as exc:
        payload = {"status": "failed", "summary": {"skill": SKILL_NAME}, "message": str(exc)}
        sys.stdout.write(pretty_json(payload, args.pretty) + "\n")
        return 1
    sys.stdout.write(pretty_json(payload, args.pretty) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
