#!/usr/bin/env python3
"""Review theme-level claim-slot sufficiency without acting as a runtime gate."""

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

SKILL_NAME = "review-theme-sufficiency"
WORKSPACE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "eco-concil-runtime").exists() and (parent / "skills").exists())
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.contracts import validate_canonical_payload  # noqa: E402
from eco_council_runtime.objects.council import (  # noqa: E402
    append_dynamic_investigation_object_record,
    query_council_objects,
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


def load_blueprint(run_dir: Path, round_id: str) -> dict[str, Any]:
    return load_json(run_dir / "reporting" / f"report_blueprint_{round_id}.json")


def load_themes(run_dir: Path, round_id: str) -> list[dict[str, Any]]:
    artifact = load_blueprint(run_dir, round_id)
    themes = [item for item in list_items(artifact.get("investigation_themes")) if isinstance(item, dict)]
    if themes:
        return themes
    try:
        payload = query_council_objects(run_dir, object_kind="investigation-theme", round_id=round_id, limit=100)
    except Exception:
        return []
    return [item for item in list_items(payload.get("objects")) if isinstance(item, dict)]


def themes_from_program(program: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in list_items(program.get("theme_threads")):
        if not isinstance(item, dict):
            continue
        theme_id = maybe_text(item.get("theme_id")) or maybe_text(item.get("active_theme_id"))
        if not theme_id:
            continue
        rows.append(
            {
                "theme_id": theme_id,
                "theme_question": maybe_text(item.get("theme_question")),
                "claim_slots_supported": list_items(item.get("claim_slots_supported")),
                "claim_boundary": maybe_text(item.get("claim_basis_boundary"))
                or maybe_text(item.get("claim_boundary")),
                "owner_role": maybe_text(item.get("owner_role")),
            }
        )
    return rows


def filter_active_themes(
    themes: list[dict[str, Any]],
    *,
    active_theme_ids: list[str],
) -> list[dict[str, Any]]:
    if not active_theme_ids:
        return themes
    active = set(active_theme_ids)
    filtered = [
        theme
        for theme in themes
        if maybe_text(theme.get("theme_id")) in active
    ]
    existing = {maybe_text(theme.get("theme_id")) for theme in filtered}
    for theme_id in active_theme_ids:
        if theme_id not in existing:
            filtered.append(
                {
                    "theme_id": theme_id,
                    "theme_question": f"Progress for {theme_id}",
                    "claim_slots_supported": [],
                }
            )
    return filtered


def signal_counts(run_dir: Path, *, run_id: str, round_id: str) -> dict[str, Any]:
    db_path = run_dir / "analytics" / "signal_plane.sqlite"
    counts = {"environment": 0, "formal": 0, "public": 0}
    source_skill_counts: Counter[str] = Counter()
    evidence_refs: list[str] = []
    if not db_path.exists():
        return {"plane_counts": counts, "source_skill_counts": [], "evidence_refs": []}
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        present = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'normalized_signals'"
        ).fetchone()
        if present is None:
            return {"plane_counts": counts, "source_skill_counts": [], "evidence_refs": []}
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
        if plane in counts:
            counts[plane] += 1
        source_skill_counts[maybe_text(row["source_skill"])] += 1
        signal_id = maybe_text(row["signal_id"])
        if signal_id:
            evidence_refs.append(f"signal:{signal_id}")
    return {
        "plane_counts": counts,
        "source_skill_counts": [
            {"source_skill": skill, "signal_count": count}
            for skill, count in sorted(source_skill_counts.items())
        ],
        "evidence_refs": evidence_refs,
    }


def load_checkpoints(run_dir: Path, round_id: str, checkpoint_path: str) -> list[dict[str, Any]]:
    path = resolve_path(run_dir, checkpoint_path, f"analytics/acquisition_checkpoints_{round_id}.json")
    payload = load_json(path)
    return [item for item in list_items(payload.get("acquisition_checkpoints")) if isinstance(item, dict)]


def load_denominators(run_dir: Path, round_id: str) -> list[Any]:
    coverage = load_json(run_dir / "analytics" / f"public_discourse_coverage_audit_{round_id}.json")
    corpus = load_json(run_dir / "analytics" / f"public_discourse_corpus_{round_id}.json")
    return list_items(coverage.get("source_family_denominators")) or list_items(corpus.get("source_family_denominators"))


def load_source_limits(run_dir: Path, round_id: str) -> list[Any]:
    rows: list[Any] = []
    for name in (
        f"public_discourse_coverage_audit_{round_id}.json",
        f"public_discourse_corpus_{round_id}.json",
    ):
        payload = load_json(run_dir / "analytics" / name)
        rows.extend(list_items(payload.get("source_limit_records")))
    return unique_values(rows)


def latest_round_brief(run_dir: Path, *, run_id: str, round_id: str) -> dict[str, Any]:
    try:
        payload = query_council_objects(
            run_dir,
            object_kind="round-brief",
            run_id=run_id,
            round_id=round_id,
            limit=1,
        )
    except Exception:
        return {}
    rows = [item for item in list_items(payload.get("objects")) if isinstance(item, dict)]
    return rows[0] if rows else {}


def latest_program(run_dir: Path, *, run_id: str, program_id: str) -> dict[str, Any]:
    if not program_id:
        return {}
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
        if maybe_text(item.get("program_id")) == program_id or maybe_text(item.get("object_id")) == program_id:
            return item
    return {}


def theme_kind(theme: dict[str, Any]) -> str:
    theme_id = maybe_text(theme.get("theme_id")).casefold()
    claim_slot_text = " ".join(
        unique_texts(list_items(theme.get("claim_slots_supported")))
    ).casefold()
    theme_text = " ".join(
        [
            maybe_text(theme.get("theme_question")),
            maybe_text(theme.get("claim_boundary")),
            claim_slot_text,
        ]
    ).casefold()
    combined = " ".join([theme_id, claim_slot_text, theme_text])
    if "interaction" in combined or "timeline" in combined:
        return "interaction"
    if "public" in combined or "semantic" in combined or "media" in combined:
        return "public_semantic"
    if "official" in combined or "policy" in combined or "governance" in combined:
        return "official_policy"
    if "fact" in combined or "environment" in combined or "event" in combined:
        return "fact_process"
    return "unknown"


def denominator_count(row: Any) -> int:
    if not isinstance(row, dict):
        return 0
    fields = (
        "denominator",
        "sample_count",
        "eligible_signal_count",
        "signal_count",
        "observed_signal_count",
        "annotated_signal_count",
        "label_family_denominator",
    )
    values: list[int] = []
    for field_name in fields:
        try:
            values.append(int(row.get(field_name) or 0))
        except (TypeError, ValueError):
            continue
    for nested_name in ("denominator_policy", "sample_definition"):
        nested = row.get(nested_name)
        if isinstance(nested, dict):
            values.append(denominator_count(nested))
    return max(values) if values else 0


def council_basis_counts(run_dir: Path, *, run_id: str, round_id: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for kind in ("finding", "evidence-bundle", "review-comment", "readiness-opinion"):
        try:
            payload = query_council_objects(run_dir, object_kind=kind, run_id=run_id, round_id=round_id, limit=200)
        except Exception:
            result[kind] = {"count": 0, "evidence_refs": [], "object_ids": []}
            continue
        objects = [item for item in list_items(payload.get("objects")) if isinstance(item, dict)]
        refs = [ref for item in objects for ref in list_items(item.get("evidence_refs"))]
        ids = [
            maybe_text(item.get("finding_id"))
            or maybe_text(item.get("bundle_id"))
            or maybe_text(item.get("comment_id"))
            or maybe_text(item.get("opinion_id"))
            for item in objects
        ]
        result[kind] = {"count": len(objects), "evidence_refs": unique_values(refs), "object_ids": unique_texts(ids)}
    return result


def theme_review(
    *,
    run_id: str,
    round_id: str,
    theme: dict[str, Any],
    signals: dict[str, Any],
    checkpoints: list[dict[str, Any]],
    denominators: list[Any],
    source_limits: list[Any],
    basis_counts: dict[str, Any],
    output_file: Path,
) -> dict[str, Any]:
    theme_id = maybe_text(theme.get("theme_id")) or "theme-round"
    claim_slots = unique_texts(list_items(theme.get("claim_slots_supported")))
    supported: list[str] = []
    unsupported: list[str] = []
    downgrades: list[dict[str, str]] = []
    plane_counts = dict_items(signals.get("plane_counts"))
    has_basis_object = bool(
        basis_counts.get("finding", {}).get("count") or basis_counts.get("evidence-bundle", {}).get("count")
    )
    kind = theme_kind(theme)
    if kind == "fact_process":
        if int(plane_counts.get("environment") or 0) > 0 or has_basis_object:
            supported.extend(claim_slots)
        else:
            unsupported.extend(claim_slots)
            downgrades.append({"claim_slot": ",".join(claim_slots), "downgrade": "fact process wording must be limited to missing basis or council-scoped open question"})
    elif kind == "official_policy":
        if int(plane_counts.get("formal") or 0) > 0:
            supported.extend(claim_slots)
        else:
            unsupported.extend(claim_slots)
            downgrades.append({"claim_slot": ",".join(claim_slots), "downgrade": "policy response/effectiveness and official-action conclusions are blocked without official action or governance record basis"})
    elif kind == "public_semantic":
        visible_denominator_count = sum(denominator_count(item) for item in denominators)
        if visible_denominator_count > 0:
            supported.extend(claim_slots)
        else:
            unsupported.extend(claim_slots)
            downgrades.append({"claim_slot": ",".join(claim_slots), "downgrade": "public semantic wording must be examples or missing-basis limitations without corpus/coverage denominators"})
    elif kind == "interaction":
        timeline = load_json(output_file.parent / f"fact_policy_public_interaction_timeline_{round_id}.json")
        nodes = [item for item in list_items(timeline.get("interaction_nodes")) if isinstance(item, dict)]
        if any(list_items(node.get("fact_or_policy_evidence_refs")) and list_items(node.get("public_or_media_evidence_refs")) for node in nodes):
            supported.extend(claim_slots)
        else:
            unsupported.extend(claim_slots)
            downgrades.append({"claim_slot": ",".join(claim_slots), "downgrade": "interaction wording must be removed or kept as separate lane chronology without lane episode cards and two ref classes"})
    else:
        if has_basis_object:
            supported.extend(claim_slots)
        else:
            unsupported.extend(claim_slots)
    matching_checkpoints = [
        checkpoint
        for checkpoint in checkpoints
        if maybe_text(checkpoint.get("theme_id")) == theme_id
    ]
    checkpoint_risks = [
        risk
        for checkpoint in matching_checkpoints
        for risk in list_items(checkpoint.get("coverage_risks"))
    ]
    evidence_refs = unique_values(
        [
            *list_items(signals.get("evidence_refs"))[:20],
            *basis_counts.get("finding", {}).get("evidence_refs", []),
            *basis_counts.get("evidence-bundle", {}).get("evidence_refs", []),
            *[ref for checkpoint in matching_checkpoints for ref in list_items(checkpoint.get("evidence_refs"))],
        ]
    )
    review = {
        "review_id": "theme-sufficiency-review-" + stable_hash(run_id, round_id, theme_id, supported, unsupported)[:12],
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "decision_source": "approved-helper-view",
        "theme_id": theme_id,
        "review_status": "supported-with-limits" if supported and not unsupported else "downgrade-required" if unsupported else "insufficient-inputs",
        "summary": "Theme sufficiency review states support/downgrade boundaries only; it is not runtime truth or report readiness.",
        "supported_claim_slots": unique_texts(supported),
        "unsupported_claim_slots": unique_texts(unsupported),
        "valid_denominators": denominators,
        "source_family_limits": unique_values([*source_limits, *checkpoint_risks]),
        "representativeness_limits": [
            "Public, media, and formal semantic denominators remain source-family-local.",
            "Small or low-volume samples can support examples or sample-local cues only.",
            "Policy evaluation basis must be synthesized from upstream lanes; it is not an acquisition lane.",
        ],
        "required_downgrades": downgrades,
        "recommended_section_brief_inputs": [
            {
                "agent_role": maybe_text(theme.get("owner_role")) or "moderator",
                "theme_id": theme_id,
                "claim_slots": claim_slots,
                "include_limitations": True,
                "include_denominators": True,
            }
        ],
        "evidence_refs": evidence_refs,
        "lineage": unique_texts([theme_id, *claim_slots, *[maybe_text(item.get("checkpoint_id")) for item in matching_checkpoints]]),
        "provenance": {
            "skill_name": SKILL_NAME,
            "decision_source": "approved-helper-view",
            "artifact_path": str(output_file),
        },
    }
    return validate_canonical_payload("theme-sufficiency-review", review)


def recommended_progress_disposition(
    review: dict[str, Any],
    *,
    program_id: str,
    round_brief_id: str,
) -> str:
    if not maybe_text(program_id) or not maybe_text(round_brief_id):
        return "blocked-by-program-mismatch"
    unsupported = list_items(review.get("unsupported_claim_slots"))
    source_limits = list_items(review.get("source_family_limits"))
    if not unsupported:
        return "satisfied-for-current-claim-strength"
    combined_limits = " ".join(maybe_text(item) for item in source_limits).casefold()
    if "scope out" in combined_limits or "scope-out" in combined_limits:
        return "scope-out-with-rationale"
    if any(term in combined_limits for term in ("no reasonable recovery", "policy lane absence", "denominator dispute", "challenger concern")):
        return "needs-supplemental-round"
    if source_limits:
        return "needs-in-round-recovery"
    return "downgrade-required"


def theme_progress_review(
    *,
    run_id: str,
    round_id: str,
    program_id: str,
    round_brief_id: str,
    theme: dict[str, Any],
    sufficiency_review: dict[str, Any],
    round_brief: dict[str, Any],
    output_file: Path,
) -> dict[str, Any]:
    theme_id = maybe_text(theme.get("theme_id")) or maybe_text(sufficiency_review.get("theme_id"))
    resolved_program_id = maybe_text(program_id) or maybe_text(round_brief.get("program_id"))
    resolved_round_brief_id = maybe_text(round_brief_id) or maybe_text(round_brief.get("object_id"))
    disposition = recommended_progress_disposition(
        sufficiency_review,
        program_id=resolved_program_id,
        round_brief_id=resolved_round_brief_id,
    )
    unsupported = list_items(sufficiency_review.get("unsupported_claim_slots"))
    supported = list_items(sufficiency_review.get("supported_claim_slots"))
    basis_refs = unique_values(
        [
            *list_items(sufficiency_review.get("evidence_refs")),
            *list_items(sufficiency_review.get("lineage")),
        ]
    )
    if disposition == "needs-in-round-recovery":
        recovery = [
            "Keep recovery inside the current issue council round through agent acquisition or analysis turns.",
            "Record a source-limit rationale only after the source owner explains revised terms, windows, provider mode, or same-family follow-up status.",
        ]
        supplemental = []
    elif disposition == "needs-supplemental-round":
        recovery = [
            "Use moderator synthesis or readiness opinion to decide whether no reasonable in-round recovery remains.",
        ]
        supplemental = [
            {
                "active_theme_id": theme_id,
                "allowed_only_after_council_uptake": True,
                "focus_refs": unique_texts(
                    [
                        f"theme:{theme_id}",
                        f"theme-progress-review:{theme_id}",
                        maybe_text(sufficiency_review.get("review_id")),
                    ]
                ),
                "round_mode": "supplemental-issue-council",
                "round_category": "supplemental-issue-deliberation",
                "round_title": f"Supplemental issue council for {theme_id}",
                "round_subtitle_question": "Which unresolved theme boundary still needs supplemental council action?",
                "round_internal_phases": [
                    "supplemental-acquisition-turns",
                    "supplemental-analysis-turns",
                    "progress-review",
                    "moderator-synthesis",
                ],
                "transition_payload_suggestion": {
                    "program_id": resolved_program_id,
                    "source_round_id": round_id,
                    "active_theme_ids": [theme_id],
                    "primary_focus_refs": unique_texts(
                        [
                            f"theme:{theme_id}",
                            f"theme-progress-review:{theme_id}",
                            maybe_text(sufficiency_review.get("review_id")),
                        ]
                    ),
                    "unresolved_responsibility_boundary_refs": [theme_id],
                    "parent_theme_progress_review_refs": [
                        "theme-progress-review-"
                        + stable_hash(run_id, round_id, theme_id, disposition)[:12]
                    ],
                    "round_mode": "supplemental-issue-council",
                    "round_category": "supplemental-issue-deliberation",
                    "round_title": f"Supplemental issue council for {theme_id}",
                    "round_subtitle_question": "Which unresolved theme boundary still needs supplemental council action?",
                    "round_internal_phases": [
                        "supplemental-acquisition-turns",
                        "supplemental-analysis-turns",
                        "progress-review",
                        "moderator-synthesis",
                    ],
                    "requires_transition_approval": True,
                    "does_not_auto_execute": True,
                },
            }
        ]
    else:
        recovery = []
        supplemental = []
    review = {
        "review_id": "theme-progress-review-" + stable_hash(run_id, round_id, theme_id, disposition)[:12],
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "decision_source": "approved-helper-view",
        "program_id": resolved_program_id or "program-not-linked",
        "round_brief_id": resolved_round_brief_id or "round-brief-not-linked",
        "active_theme_id": theme_id,
        "summary": "Theme progress review is advisory only and cannot open a round, certify truth, or authorize report use.",
        "analysis_status": "analysis-ready"
        if disposition == "satisfied-for-current-claim-strength"
        else "requires-in-round-recovery"
        if disposition == "needs-in-round-recovery"
        else "requires-supplemental-council-uptake"
        if disposition == "needs-supplemental-round"
        else "requires-downgrade-or-scope-decision",
        "agent_responsibility_status": [
            f"supported claim slots: {', '.join(unique_texts(supported)) or 'none'}",
            f"unsupported claim slots: {', '.join(unique_texts(unsupported)) or 'none'}",
        ],
        "available_basis_refs": basis_refs,
        "denominator_status": "visible-or-not-applicable"
        if list_items(sufficiency_review.get("valid_denominators"))
        else "missing-or-not-yet-carried",
        "coverage_or_policy_lane_limits": list_items(sufficiency_review.get("source_family_limits")),
        "in_round_recovery_options": recovery,
        "recommended_disposition": disposition,
        "supplemental_round_recommendation": supplemental,
        "evidence_refs": list_items(sufficiency_review.get("evidence_refs")),
        "lineage": unique_texts(
            [
                theme_id,
                maybe_text(program_id),
                maybe_text(round_brief_id),
                maybe_text(sufficiency_review.get("review_id")),
                *list_items(round_brief.get("active_theme_ids")),
            ]
        ),
        "provenance": {
            "skill_name": SKILL_NAME,
            "decision_source": "approved-helper-view",
            "artifact_path": str(output_file),
        },
    }
    return validate_canonical_payload("theme-progress-review", review)


def review_theme_sufficiency(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = resolve_run_dir(args.run_dir)
    output_file = resolve_path(run_dir, args.output_path, f"analytics/theme_sufficiency_review_{args.round_id}.json")
    round_brief = latest_round_brief(run_dir, run_id=args.run_id, round_id=args.round_id)
    program_id = maybe_text(args.program_id) or maybe_text(round_brief.get("program_id"))
    program = latest_program(run_dir, run_id=args.run_id, program_id=program_id)
    active_theme_ids = unique_texts(
        [
            *list_items(round_brief.get("active_theme_ids")),
            *list_items(args.active_theme_id),
        ]
    )
    themes = load_themes(run_dir, args.round_id)
    if not themes and program:
        themes = themes_from_program(program)
    themes = filter_active_themes(themes, active_theme_ids=active_theme_ids)
    if maybe_text(args.theme_id):
        themes = [theme for theme in themes if maybe_text(theme.get("theme_id")) == maybe_text(args.theme_id)]
    if not themes:
        themes = [{"theme_id": "theme-round", "theme_question": "Round-level report claim support", "claim_slots_supported": []}]
    signals = signal_counts(run_dir, run_id=args.run_id, round_id=args.round_id)
    checkpoints = load_checkpoints(run_dir, args.round_id, args.checkpoint_path)
    denominators = load_denominators(run_dir, args.round_id)
    source_limits = load_source_limits(run_dir, args.round_id)
    basis_counts = council_basis_counts(run_dir, run_id=args.run_id, round_id=args.round_id)
    reviews = [
        theme_review(
            run_id=args.run_id,
            round_id=args.round_id,
            theme=theme,
            signals=signals,
            checkpoints=checkpoints,
            denominators=denominators,
            source_limits=source_limits,
            basis_counts=basis_counts,
            output_file=output_file,
        )
        for theme in themes
    ]
    progress_reviews = [
        theme_progress_review(
            run_id=args.run_id,
            round_id=args.round_id,
            program_id=program_id,
            round_brief_id=maybe_text(args.round_brief_id) or maybe_text(round_brief.get("object_id")),
            theme=theme,
            sufficiency_review=review,
            round_brief=round_brief,
            output_file=output_file,
        )
        for theme, review in zip(themes, reviews)
    ]
    stored_progress_reviews: list[dict[str, Any]] = []
    for review in progress_reviews:
        review_payload = {
            **review,
            "object_kind": "theme-progress-review",
            "author_role": "moderator",
            "status": "advisory",
            "target_kind": "investigation-theme",
            "target_id": maybe_text(review.get("active_theme_id")),
            "target": {
                "object_kind": "investigation-theme",
                "object_id": maybe_text(review.get("active_theme_id")),
            },
            "rationale": maybe_text(review.get("summary")),
        }
        append_result = append_dynamic_investigation_object_record(
            run_dir,
            object_payload=review_payload,
            object_kind="theme-progress-review",
            artifact_path=str(output_file),
            record_locator="$.theme_progress_reviews",
        )
        stored_review = dict_items(append_result.get("object"))
        stored_progress_reviews.append(stored_review or review)
    progress_reviews = stored_progress_reviews
    wrapper = {
        "schema_version": "theme-sufficiency-review-materialization-v1",
        "skill": SKILL_NAME,
        "run_id": args.run_id,
        "round_id": args.round_id,
        "program_id": program_id,
        "round_brief_id": maybe_text(args.round_brief_id) or maybe_text(round_brief.get("object_id")),
        "active_theme_ids": active_theme_ids,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "theme_sufficiency_reviews": reviews,
        "theme_progress_reviews": progress_reviews,
        "supported_claim_slots": unique_texts([slot for review in reviews for slot in list_items(review.get("supported_claim_slots"))]),
        "unsupported_claim_slots": unique_texts([slot for review in reviews for slot in list_items(review.get("unsupported_claim_slots"))]),
        "valid_denominators": unique_values([item for review in reviews for item in list_items(review.get("valid_denominators"))]),
        "source_family_limits": unique_values([item for review in reviews for item in list_items(review.get("source_family_limits"))]),
        "required_downgrades": unique_values([item for review in reviews for item in list_items(review.get("required_downgrades"))]),
        "review_policy": {
            "not_runtime_gate": True,
            "does_not_certify_truth": True,
            "requires_council_or_report_basis_uptake": True,
            "does_not_open_supplemental_round": True,
            "ordinary_query_repair_stays_in_round": True,
        },
        "review_filter_context": {
            "signal_scope": "run_id+round_id",
            "theme_id": maybe_text(args.theme_id),
        },
        "provenance": {"skill_name": SKILL_NAME, "decision_source": "approved-helper-view"},
        "artifact_refs": [
            {
                "artifact_path": str(output_file),
                "record_locator": "$.theme_sufficiency_reviews",
                "artifact_ref": f"{output_file}:$.theme_sufficiency_reviews",
            },
            {
                "artifact_path": str(output_file),
                "record_locator": "$.theme_progress_reviews",
                "artifact_ref": f"{output_file}:$.theme_progress_reviews",
            }
        ],
        "warnings": [],
    }
    write_json(output_file, wrapper)
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": args.run_id,
            "round_id": args.round_id,
            "output_path": str(output_file),
            "review_count": len(reviews),
            "progress_review_count": len(progress_reviews),
            "unsupported_claim_slot_count": len(wrapper["unsupported_claim_slots"]),
        },
        "receipt_id": "theme-sufficiency-review-receipt-" + stable_hash(args.run_id, args.round_id, output_file)[:20],
        "artifact_refs": wrapper["artifact_refs"],
        "canonical_ids": [
            *[maybe_text(item.get("review_id")) for item in reviews],
            *[maybe_text(item.get("review_id")) for item in progress_reviews],
        ],
        "warnings": [],
        "board_handoff": {
            "candidate_ids": [maybe_text(item.get("review_id")) for item in reviews],
            "theme_progress_review_ids": [
                maybe_text(item.get("review_id")) for item in progress_reviews
            ],
            "gap_hints": [
                maybe_text(item.get("downgrade"))
                for review in reviews
                for item in list_items(review.get("required_downgrades"))
                if isinstance(item, dict)
            ],
            "suggested_next_skills": [],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Review theme-level sufficiency without runtime gating.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--program-id", default="")
    parser.add_argument("--round-brief-id", default="")
    parser.add_argument("--active-theme-id", action="append", default=[])
    parser.add_argument("--theme-id", default="")
    parser.add_argument("--checkpoint-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = review_theme_sufficiency(args)
    sys.stdout.write(pretty_json(payload, args.pretty) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
