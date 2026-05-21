#!/usr/bin/env python3
"""Draft and store one agent-authored report section brief."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "draft-agent-section-brief"
WORKSPACE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "eco-concil-runtime").exists() and (parent / "skills").exists())
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.objects.council import query_council_objects  # noqa: E402
from eco_council_runtime.reporting_objects import store_agent_section_brief_record  # noqa: E402


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


def parse_json_list_option(text: str, *, option_name: str) -> list[Any]:
    if not maybe_text(text):
        return []
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{option_name} must be valid JSON.") from exc
    if not isinstance(payload, list):
        raise ValueError(f"{option_name} must be a JSON list.")
    return payload


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


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


def source_family_for_skill(source_skill: str, plane: str) -> str:
    if source_skill.startswith("fetch-gdelt"):
        return "gdelt-public-record"
    if source_skill.startswith("fetch-youtube"):
        return "youtube-public-discourse"
    if source_skill.startswith("fetch-bluesky"):
        return "bluesky-public-discourse"
    if source_skill.startswith("fetch-regulationsgov"):
        return "regulationsgov-formal-comments"
    if source_skill in {"fetch-federal-register-documents", "fetch-epa-eis-records", "fetch-usbr-project-records"}:
        return "formal-record"
    if plane == "environment":
        return "environment-observation"
    if plane == "formal":
        return "formal-record"
    if plane == "public":
        return "public-discourse"
    return "other"


def signal_summary(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    allowed_planes: set[str] | None = None,
) -> dict[str, Any]:
    db_path = run_dir / "analytics" / "signal_plane.sqlite"
    plane_counts: Counter[str] = Counter()
    family_counts: Counter[str] = Counter()
    refs: list[str] = []
    if not db_path.exists():
        return {"plane_counts": {}, "source_family_counts": [], "evidence_refs": []}
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        present = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'normalized_signals'"
        ).fetchone()
        if present is None:
            return {"plane_counts": {}, "source_family_counts": [], "evidence_refs": []}
        rows = connection.execute(
            """
            SELECT signal_id, plane, source_skill
            FROM normalized_signals
            WHERE run_id = ? AND round_id = ?
            """,
            (run_id, round_id),
        ).fetchall()
    finally:
        connection.close()
    for row in rows:
        plane = maybe_text(row["plane"])
        if allowed_planes is not None and plane not in allowed_planes:
            continue
        source_skill = maybe_text(row["source_skill"])
        plane_counts[plane] += 1
        family_counts[source_family_for_skill(source_skill, plane)] += 1
        signal_id = maybe_text(row["signal_id"])
        if signal_id:
            refs.append(f"signal:{signal_id}")
    return {
        "plane_counts": dict(sorted(plane_counts.items())),
        "source_family_counts": [
            {"source_family": family, "signal_count": count}
            for family, count in sorted(family_counts.items())
        ],
        "evidence_refs": refs,
    }


def council_objects(run_dir: Path, *, run_id: str, round_id: str, agent_role: str) -> dict[str, list[dict[str, Any]]]:
    rows: dict[str, list[dict[str, Any]]] = {}
    for kind in ("finding", "evidence-bundle", "review-comment", "readiness-opinion"):
        kwargs: dict[str, Any] = {"object_kind": kind, "run_id": run_id, "round_id": round_id, "limit": 200}
        if kind in {"finding", "evidence-bundle", "readiness-opinion"} and maybe_text(agent_role):
            kwargs["agent_role"] = agent_role
        try:
            payload = query_council_objects(run_dir, **kwargs)
        except Exception:
            rows[kind] = []
            continue
        rows[kind] = [item for item in list_items(payload.get("objects")) if isinstance(item, dict)]
    return rows


def load_sufficiency_reviews(run_dir: Path, round_id: str, path_text: str) -> list[dict[str, Any]]:
    path = resolve_path(run_dir, path_text, f"analytics/theme_sufficiency_review_{round_id}.json")
    payload = load_json(path)
    return [item for item in list_items(payload.get("theme_sufficiency_reviews")) if isinstance(item, dict)]


def load_progress_reviews(run_dir: Path, round_id: str, path_text: str) -> list[dict[str, Any]]:
    path = resolve_path(run_dir, path_text, f"analytics/theme_sufficiency_review_{round_id}.json")
    payload = load_json(path)
    return [item for item in list_items(payload.get("theme_progress_reviews")) if isinstance(item, dict)]


def load_latest_program(run_dir: Path, *, run_id: str, program_id: str) -> dict[str, Any]:
    try:
        payload = query_council_objects(
            run_dir,
            object_kind="council-investigation-program",
            run_id=run_id,
            limit=50,
        )
    except Exception:
        return {}
    for item in list_items(payload.get("objects")):
        if not isinstance(item, dict):
            continue
        if not program_id or maybe_text(item.get("program_id")) == program_id or maybe_text(item.get("object_id")) == program_id:
            return item
    return {}


def role_section_defaults(agent_role: str) -> tuple[str, str]:
    if agent_role == "environmental-investigator":
        return "fact_event_process", "Environmental/fact process brief"
    if agent_role == "social-investigator":
        return "public_semantic_and_policy_record", "Public, media, formal, and policy-record brief"
    if agent_role == "moderator":
        return "official_action_policy_record", "Official action, sufficiency, and policy-boundary brief"
    if agent_role == "challenger":
        return "challenger_limitations", "Challenger limitations and blocked wording brief"
    return "agent_section", f"{agent_role} section brief"


def signal_planes_for_brief(agent_role: str, section_key: str) -> set[str] | None:
    section_text = maybe_text(section_key).casefold()
    if "interaction" in section_text or "timeline" in section_text:
        return {"environment", "formal", "public"}
    if "public" in section_text or "semantic" in section_text or "media" in section_text:
        return {"public", "formal"}
    if "official" in section_text or "policy" in section_text or "governance" in section_text:
        return {"formal"}
    if "fact" in section_text or "environment" in section_text or "event" in section_text:
        return {"environment", "formal"}
    if agent_role == "environmental-investigator":
        return {"environment", "formal"}
    if agent_role == "social-investigator":
        return {"public", "formal"}
    if agent_role == "moderator":
        return {"formal"}
    return None


def relevant_reviews(reviews: list[dict[str, Any]], agent_role: str, section_key: str) -> list[dict[str, Any]]:
    text_terms = {
        "environmental-investigator": ("fact", "environment", "event"),
        "social-investigator": ("public", "semantic", "media", "official", "policy", "governance"),
        "moderator": ("official", "policy", "governance", "interaction", "timeline"),
        "challenger": ("",),
    }.get(agent_role, ("",))
    if "official" in section_key or "policy" in section_key:
        text_terms = ("official", "policy", "governance")
    selected: list[dict[str, Any]] = []
    for review in reviews:
        haystack = " ".join(
            [
                maybe_text(review.get("theme_id")),
                " ".join(list_items(review.get("supported_claim_slots"))),
                " ".join(list_items(review.get("unsupported_claim_slots"))),
                json.dumps(review.get("required_downgrades", []), ensure_ascii=True),
            ]
        ).casefold()
        if any(term and term in haystack for term in text_terms):
            selected.append(review)
    return selected or reviews


def relevant_progress_reviews(reviews: list[dict[str, Any]], theme_ids: list[str]) -> list[dict[str, Any]]:
    if not theme_ids:
        return reviews
    active = set(theme_ids)
    return [
        review
        for review in reviews
        if maybe_text(review.get("active_theme_id")) in active
    ] or reviews


def object_text(item: dict[str, Any]) -> str:
    for field_name in ("summary", "title", "rationale", "opinion_text", "comment_text"):
        text = maybe_text(item.get(field_name))
        if text:
            return text
    return ""


def build_brief(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = resolve_run_dir(args.run_dir)
    section_key, default_role = role_section_defaults(args.agent_role)
    section_key = maybe_text(args.section_key) or section_key
    section_role = maybe_text(args.section_role) or default_role
    output_file = resolve_path(
        run_dir,
        args.output_path,
        f"reporting/agent_section_brief_{args.agent_role}_{section_key}_{args.round_id}.json",
    )
    reviews = relevant_reviews(
        load_sufficiency_reviews(run_dir, args.round_id, args.sufficiency_review_path),
        args.agent_role,
        section_key,
    )
    progress_reviews = relevant_progress_reviews(
        load_progress_reviews(run_dir, args.round_id, args.sufficiency_review_path),
        unique_texts([maybe_text(review.get("theme_id")) for review in reviews]),
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
    signals = signal_summary(
        run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        allowed_planes=signal_planes_for_brief(args.agent_role, section_key),
    )
    objects = council_objects(run_dir, run_id=args.run_id, round_id=args.round_id, agent_role=args.agent_role)
    object_claims = unique_texts([object_text(item) for items in objects.values() for item in items])[:5]
    supported_slots = unique_texts([slot for review in reviews for slot in list_items(review.get("supported_claim_slots"))])
    unsupported_slots = unique_texts([slot for review in reviews for slot in list_items(review.get("unsupported_claim_slots"))])
    review_downgrades = [
        maybe_text(item.get("downgrade")) if isinstance(item, dict) else maybe_text(item)
        for review in reviews
        for item in list_items(review.get("required_downgrades"))
    ]
    progress_limits = [
        maybe_text(item)
        for review in progress_reviews
        for item in list_items(review.get("coverage_or_policy_lane_limits"))
    ]
    if supported_slots:
        strength = "bounded-supported"
    elif unsupported_slots:
        strength = "insufficient-basis-downgrade-required"
    else:
        strength = "brief-only-no-sufficiency-review"
    if maybe_text(args.claim_strength):
        strength = maybe_text(args.claim_strength)
    main_claims = unique_texts(
        [
            *parse_json_list_option(args.main_claims_json, option_name="--main-claims-json"),
            *object_claims,
            *[
                f"Can support claim slot {slot} only within cited source and denominator boundaries."
                for slot in supported_slots
            ],
            *[
                f"Cannot support claim slot {slot} without downgrade or further council-carried basis."
                for slot in unsupported_slots
            ],
        ]
    )
    if not main_claims:
        main_claims = ["No supportable substantive claim was identified for this agent brief; use as limitation context only."]
    limitations = unique_texts(
        [
            *parse_json_list_option(args.limitations_json, option_name="--limitations-json"),
            *review_downgrades,
            *progress_limits,
            "This agent section brief is not a runtime gate and does not certify truth.",
            "Report use requires frozen/reporting basis or council-carried uptake.",
        ]
    )
    blocked = unique_texts(
        [
            *parse_json_list_option(args.blocked_phrases_json, option_name="--blocked-phrases-json"),
            "policy was effective",
            "public opinion shows",
            "the public mostly",
            "official action caused public response",
            "absence of evidence proves absence",
        ]
    )
    evidence_refs = unique_values(
        [
            *parse_json_list_option(args.evidence_refs_json, option_name="--evidence-refs-json"),
            *signals.get("evidence_refs", [])[:20],
            *[ref for items in objects.values() for item in items for ref in list_items(item.get("evidence_refs"))],
            *[ref for review in reviews for ref in list_items(review.get("evidence_refs"))],
            *[ref for review in progress_reviews for ref in list_items(review.get("evidence_refs"))],
        ]
    )
    source_families = unique_texts(
        [
            *parse_json_list_option(args.source_families_json, option_name="--source-families-json"),
            *[
                maybe_text(item.get("source_family"))
                for item in list_items(signals.get("source_family_counts"))
                if isinstance(item, dict)
            ],
        ]
    )
    denominators = {
        "signal_plane_counts": dict_items(signals.get("plane_counts")),
        "source_family_counts": list_items(signals.get("source_family_counts")),
        "valid_denominators": unique_values([item for review in reviews for item in list_items(review.get("valid_denominators"))]),
        "denominator_boundary": "Public and formal semantic percentages require source-family-local denominators; do not mix families.",
    }
    basis_ids = unique_texts(
        [
            maybe_text(item.get("finding_id"))
            or maybe_text(item.get("bundle_id"))
            or maybe_text(item.get("comment_id"))
            or maybe_text(item.get("opinion_id"))
            for items in objects.values()
            for item in items
        ]
    )
    brief = {
        "brief_id": maybe_text(args.brief_id) or "agent-section-brief-" + stable_hash(args.run_id, args.round_id, args.agent_role, section_key)[:12],
        "run_id": args.run_id,
        "round_id": args.round_id,
        "generated_at_utc": utc_now_iso(),
        "decision_source": maybe_text(args.agent_role),
        "agent_role": maybe_text(args.agent_role),
        "status": "submitted",
        "section_key": section_key,
        "section_role": section_role,
        "main_claims": main_claims,
        "evidence_refs": evidence_refs,
        "source_families": source_families,
        "claim_strength": strength,
        "denominators": denominators,
        "limitations": limitations,
        "recommended_report_use": maybe_text(args.recommended_report_use) or "Report-editor may use this brief only with frozen/reporting basis and must preserve limitations.",
        "blocked_phrases": blocked,
        "program_id": program_id,
        "claim_slots_supported": supported_slots,
        "theme_ids": unique_texts(
            [
                *[maybe_text(review.get("theme_id")) for review in reviews],
                *[maybe_text(review.get("active_theme_id")) for review in progress_reviews],
            ]
        ),
        "basis_object_ids": basis_ids,
        "sufficiency_review_ids": unique_texts([maybe_text(review.get("review_id")) for review in reviews]),
        "theme_progress_review_ids": unique_texts(
            [maybe_text(review.get("review_id")) for review in progress_reviews]
        ),
        "lineage": unique_texts(
            [
                *basis_ids,
                *supported_slots,
                *unsupported_slots,
                maybe_text(program.get("object_id")),
                *[maybe_text(review.get("review_id")) for review in reviews],
                *[maybe_text(review.get("review_id")) for review in progress_reviews],
            ]
        ),
        "provenance": {
            "skill_name": SKILL_NAME,
            "decision_source": maybe_text(args.agent_role),
            "artifact_path": str(output_file),
        },
    }
    store_result = store_agent_section_brief_record(
        run_dir,
        brief_payload=brief,
        artifact_path=str(output_file),
    )
    stored_brief = dict_items(store_result.get("brief"))
    wrapper = {
        "schema_version": "agent-section-brief-draft-v1",
        "skill": SKILL_NAME,
        "run_id": args.run_id,
        "round_id": args.round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "brief": stored_brief,
        "db_path": maybe_text(store_result.get("db_path")),
        "artifact_refs": [
            {
                "artifact_path": str(output_file),
                "record_locator": "$.brief",
                "artifact_ref": f"{output_file}:$.brief",
            }
        ],
        "provenance": {"skill_name": SKILL_NAME, "decision_source": maybe_text(args.agent_role)},
    }
    write_json(output_file, wrapper)
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": args.run_id,
            "round_id": args.round_id,
            "brief_id": maybe_text(stored_brief.get("brief_id")),
            "agent_role": maybe_text(stored_brief.get("agent_role")),
            "claim_strength": maybe_text(stored_brief.get("claim_strength")),
            "output_path": str(output_file),
            "db_path": maybe_text(store_result.get("db_path")),
        },
        "receipt_id": "agent-section-brief-receipt-" + stable_hash(args.run_id, args.round_id, stored_brief.get("brief_id"))[:20],
        "artifact_refs": wrapper["artifact_refs"],
        "canonical_ids": [maybe_text(stored_brief.get("brief_id"))],
        "warnings": [],
        "reporting_handoff": {
            "brief_ref": {"object_kind": "agent-section-brief", "object_id": maybe_text(stored_brief.get("brief_id"))},
            "blocked_phrases": list_items(stored_brief.get("blocked_phrases")),
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Draft one agent section brief for reporting handoff.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--agent-role", required=True)
    parser.add_argument("--brief-id", default="")
    parser.add_argument("--section-key", default="")
    parser.add_argument("--section-role", default="")
    parser.add_argument("--program-id", default="")
    parser.add_argument("--claim-strength", default="")
    parser.add_argument("--main-claims-json", default="")
    parser.add_argument("--evidence-refs-json", default="")
    parser.add_argument("--source-families-json", default="")
    parser.add_argument("--limitations-json", default="")
    parser.add_argument("--blocked-phrases-json", default="")
    parser.add_argument("--recommended-report-use", default="")
    parser.add_argument("--sufficiency-review-path", default="")
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
