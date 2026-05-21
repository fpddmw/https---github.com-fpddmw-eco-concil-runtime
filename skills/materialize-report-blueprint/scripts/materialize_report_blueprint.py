#!/usr/bin/env python3
"""Materialize report-framing blueprint and investigation themes."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "materialize-report-blueprint"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.objects.council import append_dynamic_investigation_object_record  # noqa: E402


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def list_items(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


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


def mission_text(run_dir: Path, round_id: str) -> str:
    mission = load_json(run_dir / "mission.json")
    scaffold = load_json(run_dir / "runtime" / f"mission_scaffold_{round_id}.json")
    pieces = [
        mission.get("topic"),
        mission.get("objective"),
        mission.get("request_text"),
        scaffold.get("topic"),
        scaffold.get("objective"),
        scaffold.get("request_text"),
    ]
    return maybe_text(" ".join(maybe_text(piece) for piece in pieces if maybe_text(piece)))


def mission_focus_label(text: str) -> str:
    cleaned = maybe_text(text)
    if not cleaned:
        return "the mission issue"
    words = cleaned.replace("/", " ").split()
    keep: list[str] = []
    stop = {
        "analyze",
        "assess",
        "evaluate",
        "report",
        "public",
        "policy",
        "situation",
        "analysis",
        "the",
        "and",
        "for",
        "with",
        "between",
        "about",
    }
    for word in words:
        stripped = word.strip(".,:;()[]{}")
        if not stripped:
            continue
        if stripped.casefold() in stop:
            continue
        keep.append(stripped)
        if len(keep) >= 8:
            break
    return " ".join(keep) or cleaned[:80]


def mission_profile(text: str) -> str:
    lowered = maybe_text(text).casefold()
    if any(term in lowered for term in ("smoke", "wildfire", "pm2.5", "air quality", "aqi")):
        return "air-quality-incident"
    if any(term in lowered for term in ("reservoir", "dam", "release", "hydropower", "colorado river", "water")):
        return "water-operations-governance"
    if any(term in lowered for term in ("formal comment", "public comment", "docket", "rulemaking", "regulations.gov", "standard")):
        return "formal-policy-comment"
    return "environment-governance"


def slot(
    *,
    slot_id: str,
    question: str,
    slot_kind: str,
    evidence_need: str,
    expected_section: str,
    boundary: str,
) -> dict[str, Any]:
    return {
        "slot_id": slot_id,
        "question": question,
        "slot_kind": slot_kind,
        "evidence_need": evidence_need,
        "expected_section": expected_section,
        "answer_boundary": boundary,
        "status": "open-question-not-conclusion",
    }


def claim_slots_for_mission(text: str) -> list[dict[str, Any]]:
    focus = mission_focus_label(text)
    profile = mission_profile(text)
    fact_label = {
        "air-quality-incident": f"What happened in the observed air-quality and smoke record for {focus}?",
        "water-operations-governance": f"What operating or hydrologic changes are visible for {focus}?",
        "formal-policy-comment": f"What formal policy or comment record is actually in scope for {focus}?",
    }.get(profile, f"What fact process or governance event is the report being asked to explain for {focus}?")
    official_label = {
        "air-quality-incident": f"What official advisories, agency actions, or governance records were visible around {focus}?",
        "water-operations-governance": f"What official operating, project, notice, or governance records frame {focus}?",
        "formal-policy-comment": f"What official docket, notice, rulemaking, or agency action frames {focus}?",
    }.get(profile, f"What official action or governance record frames {focus}?")
    public_label = {
        "air-quality-incident": f"What sample-local public or media semantics are visible about risk, communication, and concern in {focus}?",
        "water-operations-governance": f"What sample-local public, media, or formal semantics are visible about tradeoffs and governance in {focus}?",
        "formal-policy-comment": f"What sample-local formal comment issues or policy semantics are visible in {focus}?",
    }.get(profile, f"What bounded public, media, or formal semantic patterns are visible for {focus}?")
    return [
        slot(
            slot_id="claim-slot-fact-process",
            question=fact_label,
            slot_kind="fact_event_process",
            evidence_need="fact records, environmental observations, formal records, or DB-backed findings that establish what is in scope",
            expected_section="case-narrative",
            boundary="Do not infer policy success, responsibility, public attitude, or source attribution from fact records alone.",
        ),
        slot(
            slot_id="claim-slot-official-action",
            question=official_label,
            slot_kind="official_policy_action",
            evidence_need="official action, agency notice, governance record, operational record, or formal policy artifact",
            expected_section="official-action-and-governance-record",
            boundary="Official record presence can establish that an action or document exists; it does not prove effectiveness.",
        ),
        slot(
            slot_id="claim-slot-public-semantics",
            question=public_label,
            slot_kind="public_semantic_perception",
            evidence_need="public-policy corpus, coverage audit, annotation or bounded semantic aggregation with source-family denominators",
            expected_section="public-discourse-semantics",
            boundary="Only sample-local statements are allowed unless the mission supplies a representative design.",
        ),
        slot(
            slot_id="claim-slot-interaction-timeline",
            question=f"How do fact/official records and public/media or formal semantics line up in time for {focus}?",
            slot_kind="interaction_timeline",
            evidence_need="lane episode cards and interaction nodes carrying at least fact/policy refs and public/media refs",
            expected_section="fact-policy-public-interaction",
            boundary="Same-window visibility does not prove causality, response attribution, policy impact, or evidence absence.",
        ),
        slot(
            slot_id="claim-slot-policy-evaluation-basis",
            question=f"What can and cannot be used as a policy evaluation basis for {focus}?",
            slot_kind="policy_evaluation_basis",
            evidence_need="synthesis from supported fact records, official actions, public/media/formal semantics, governance records, and challenger-visible limitations",
            expected_section="policy-evaluation-basis",
            boundary="This is a report synthesis boundary, not an acquisition lane and not a policy score.",
        ),
    ]


def theme(
    *,
    theme_id: str,
    owner_role: str,
    question: str,
    slots: list[str],
    boundary: str,
    expected_artifacts: list[str],
    completion: list[str],
) -> dict[str, Any]:
    return {
        "theme_id": theme_id,
        "theme_question": question,
        "owner_role": owner_role,
        "claim_slots_supported": slots,
        "claim_boundary": boundary,
        "expected_artifacts": expected_artifacts,
        "completion_criteria": completion,
        "source_selection_policy": (
            "theme defines evidence need only; it must not preselect source "
            "families, source skills, query variants, route rankings, or query "
            "parameters"
        ),
    }


def themes_for_slots(slots: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_kind = {maybe_text(item.get("slot_kind")): maybe_text(item.get("slot_id")) for item in slots}
    return [
        theme(
            theme_id="theme-fact-event-process",
            owner_role="environmental-investigator",
            question=next(item["question"] for item in slots if item["slot_kind"] == "fact_event_process"),
            slots=[by_kind["fact_event_process"]],
            boundary="Describe fact process and physical/governance observations without upgrading to responsibility, policy effect, or public attitude.",
            expected_artifacts=["finding", "evidence-bundle", "environment or formal signal query result", "agent-section-brief"],
            completion=["item-level refs visible", "claim strength and limitations stated"],
        ),
        theme(
            theme_id="theme-official-policy-action",
            owner_role="social-investigator",
            question=next(item["question"] for item in slots if item["slot_kind"] == "official_policy_action"),
            slots=[by_kind["official_policy_action"]],
            boundary="Establish official/governance record presence and scope without proving policy effectiveness.",
            expected_artifacts=["official/governance record refs", "finding or evidence-bundle", "agent-section-brief"],
            completion=["official action or governance record basis visible, or source-limit downgrade recorded"],
        ),
        theme(
            theme_id="theme-public-semantic-perception",
            owner_role="social-investigator",
            question=next(item["question"] for item in slots if item["slot_kind"] == "public_semantic_perception"),
            slots=[by_kind["public_semantic_perception"]],
            boundary="Keep public/media/formal semantic claims source-family-local and denominator-bounded.",
            expected_artifacts=["public-policy corpus", "coverage audit", "annotation or aggregation artifact", "agent-section-brief"],
            completion=["sample definition, source family, denominator, and representativeness limits visible"],
        ),
        theme(
            theme_id="theme-interaction-timeline",
            owner_role="moderator",
            question=next(item["question"] for item in slots if item["slot_kind"] == "interaction_timeline"),
            slots=[by_kind["interaction_timeline"]],
            boundary="Compose lane chronology only after lane episode cards exist; do not infer causality from co-visibility.",
            expected_artifacts=["lane episode cards", "interaction timeline nodes", "agent-section-brief"],
            completion=["fact/policy refs and public/media refs are both visible, or interaction claim is downgraded"],
        ),
    ]


def blueprint_payload(
    *,
    run_id: str,
    round_id: str,
    author_role: str,
    text: str,
    output_file: Path,
) -> dict[str, Any]:
    slots = claim_slots_for_mission(text)
    questions = [slot_item["question"] for slot_item in slots]
    blueprint_id = "report-blueprint-" + stable_hash(run_id, round_id, text, len(slots))[:12]
    return {
        "run_id": run_id,
        "round_id": round_id,
        "object_kind": "report-blueprint",
        "object_id": blueprint_id,
        "blueprint_id": blueprint_id,
        "author_role": author_role,
        "agent_role": author_role,
        "decision_source": "report-framing-round",
        "status": "framed",
        "target_kind": "round",
        "target_id": round_id,
        "target": {"object_kind": "round", "object_id": round_id},
        "rationale": "Mission-driven report framing questions; no fetch, source route, or conclusion is selected.",
        "report_questions": questions,
        "claim_slots": slots,
        "required_evidence_families": [
            "fact and environmental record basis",
            "official action or governance record basis",
            "public/media/formal semantic corpus with source-family denominator",
            "interaction lane episode cards before timeline synthesis",
        ],
        "forbidden_claims_without_basis": [
            "policy effectiveness or policy response conclusion without official action/governance record and public-policy semantic basis",
            "public percentage or public opinion wording without source-family denominator and representativeness boundary",
            "interaction, causality, or public response attribution without lane episode cards and at least two ref classes",
            "policy_evaluation_basis as an independent acquisition lane",
        ],
        "expected_sections": unique_texts([slot_item["expected_section"] for slot_item in slots]),
        "policy_evaluation_boundaries": {
            "synthesis_layer_only": True,
            "not_acquisition_theme": True,
            "requires_upstream_basis": [
                "fact_event_process",
                "official_policy_action",
                "public_semantic_perception",
                "interaction_timeline",
            ],
        },
        "framing_participation_policy": {
            "agent_positions_expected_before_investigation": [
                "environmental-investigator",
                "social-investigator",
                "challenger",
            ],
            "advisory_only": True,
            "semantics": (
                "The blueprint is report-editor framing context until other "
                "roles adopt, narrow, or challenge it through council objects. "
                "Those positions still do not choose data sources or routes."
            ),
        },
        "evidence_refs": [],
        "lineage": [round_id],
        "provenance": {
            "source_skill": SKILL_NAME,
            "decision_source": "report-framing-round",
            "artifact_path": str(output_file),
            "mission_profile": mission_profile(text),
        },
    }


def theme_payloads(
    *,
    run_id: str,
    round_id: str,
    author_role: str,
    blueprint_id: str,
    slots: list[dict[str, Any]],
    output_file: Path,
) -> list[dict[str, Any]]:
    rows = []
    for item in themes_for_slots(slots):
        object_id = item["theme_id"] + "-" + stable_hash(run_id, round_id, blueprint_id, item["theme_id"])[:8]
        rows.append(
            {
                "run_id": run_id,
                "round_id": round_id,
                "object_kind": "investigation-theme",
                "object_id": object_id,
                "theme_id": item["theme_id"],
                "author_role": author_role,
                "agent_role": author_role,
                "decision_source": "report-framing-round",
                "status": "open",
                "target_kind": "report-blueprint",
                "target_id": blueprint_id,
                "target": {"object_kind": "report-blueprint", "object_id": blueprint_id},
                "rationale": item["theme_question"],
                "theme_question": item["theme_question"],
                "owner_role": item["owner_role"],
                "claim_slots_supported": item["claim_slots_supported"],
                "claim_boundary": item["claim_boundary"],
                "expected_artifacts": item["expected_artifacts"],
                "completion_criteria": item["completion_criteria"],
                "source_selection_policy": item["source_selection_policy"],
                "evidence_refs": [],
                "lineage": [blueprint_id, *item["claim_slots_supported"]],
                "provenance": {
                    "source_skill": SKILL_NAME,
                    "decision_source": "report-framing-round",
                    "artifact_path": str(output_file),
                },
            }
        )
    return rows


def materialize_report_blueprint(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = resolve_run_dir(args.run_dir)
    output_file = resolve_path(run_dir, args.output_path, f"reporting/report_blueprint_{args.round_id}.json")
    text = maybe_text(args.mission_text) or mission_text(run_dir, args.round_id)
    if not text:
        raise ValueError("No mission text is visible; provide --mission-text or mission.json.")
    blueprint = blueprint_payload(
        run_id=args.run_id,
        round_id=args.round_id,
        author_role=args.author_role,
        text=text,
        output_file=output_file,
    )
    themes = theme_payloads(
        run_id=args.run_id,
        round_id=args.round_id,
        author_role=args.author_role,
        blueprint_id=blueprint["blueprint_id"],
        slots=list_items(blueprint.get("claim_slots")),
        output_file=output_file,
    )
    stored_ids: list[str] = []
    for payload in [blueprint, *themes]:
        result = append_dynamic_investigation_object_record(
            run_dir,
            object_payload=payload,
            object_kind=maybe_text(payload.get("object_kind")),
            artifact_path=str(output_file),
            record_locator="$.report_blueprint" if payload is blueprint else "$.investigation_themes",
        )
        obj = result.get("object") if isinstance(result.get("object"), dict) else {}
        stored_ids.append(maybe_text(obj.get("object_id")))
        if payload is blueprint:
            blueprint = obj
        else:
            index = themes.index(payload)
            themes[index] = obj
    wrapper = {
        "schema_version": "report-framing-blueprint-materialization-v1",
        "skill": SKILL_NAME,
        "run_id": args.run_id,
        "round_id": args.round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "mission_focus": text,
        "report_blueprint": blueprint,
        "claim_slots": list_items(blueprint.get("claim_slots")),
        "investigation_themes": themes,
        "synthesis_targets": [
            {
                "target_id": "policy_evaluation_basis",
                "target_kind": "report-synthesis-layer",
                "not_acquisition_theme": True,
                "requires_upstream_theme_ids": [
                    "theme-fact-event-process",
                    "theme-official-policy-action",
                    "theme-public-semantic-perception",
                    "theme-interaction-timeline",
                ],
            }
        ],
        "framing_boundaries": [
            "No fetch, source family choice, query, skill route, or conclusion was selected by this framing output.",
            "Claim slots are mission-driven questions to answer, not fixed templates or expected conclusions.",
            "Investigation themes define claim-basis needs only; investigators author or adopt obligation plans without source/query precommitment.",
            "Before investigation, role positions should adopt, narrow, or challenge the framing so the split is council-visible.",
        ],
        "artifact_refs": [
            {
                "artifact_path": str(output_file),
                "record_locator": "$.report_blueprint",
                "artifact_ref": f"{output_file}:$.report_blueprint",
            }
        ],
        "provenance": {
            "source_skill": SKILL_NAME,
            "decision_source": "report-framing-round",
        },
    }
    write_json(output_file, wrapper)
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": args.run_id,
            "round_id": args.round_id,
            "output_path": str(output_file),
            "blueprint_id": maybe_text(blueprint.get("blueprint_id")),
            "claim_slot_count": len(list_items(blueprint.get("claim_slots"))),
            "investigation_theme_count": len(themes),
        },
        "receipt_id": "report-blueprint-receipt-" + stable_hash(args.run_id, args.round_id, output_file)[:20],
        "artifact_refs": wrapper["artifact_refs"],
        "canonical_ids": unique_texts(stored_ids),
        "warnings": [],
        "council_handoff": {
            "object_refs": [
                {"object_kind": "report-blueprint", "object_id": maybe_text(blueprint.get("object_id"))},
                *[
                    {"object_kind": "investigation-theme", "object_id": maybe_text(theme.get("object_id"))}
                    for theme in themes
                ],
            ],
            "suggested_next_skills": ["submit-theme-acquisition-plan"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize mission-driven report blueprint and investigation themes.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--author-role", default="moderator")
    parser.add_argument("--mission-text", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        payload = materialize_report_blueprint(args)
    except ValueError as exc:
        payload = {"status": "failed", "summary": {"skill": SKILL_NAME}, "message": str(exc)}
        sys.stdout.write(pretty_json(payload, args.pretty) + "\n")
        return 1
    sys.stdout.write(pretty_json(payload, args.pretty) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
