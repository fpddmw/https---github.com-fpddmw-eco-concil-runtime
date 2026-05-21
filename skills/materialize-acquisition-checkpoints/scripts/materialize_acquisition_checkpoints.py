#!/usr/bin/env python3
"""Materialize lightweight in-round acquisition checkpoints."""

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

SKILL_NAME = "materialize-acquisition-checkpoints"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.contracts import validate_canonical_payload  # noqa: E402
from eco_council_runtime.objects.council import query_council_objects  # noqa: E402


PUBLIC_FAMILY_BY_SKILL = {
    "fetch-gdelt-doc-search": "gdelt-public-record",
    "fetch-gdelt-events": "gdelt-public-record",
    "fetch-gdelt-mentions": "gdelt-public-record",
    "fetch-gdelt-gkg": "gdelt-public-record",
    "fetch-youtube-video-search": "youtube-public-discourse",
    "fetch-youtube-comments": "youtube-public-discourse",
    "fetch-bluesky-cascade": "bluesky-public-discourse",
    "fetch-regulationsgov-comments": "regulationsgov-formal-comments",
    "fetch-regulationsgov-comment-detail": "regulationsgov-formal-comments",
    "fetch-regulationsgov-attachments": "regulationsgov-formal-comments",
    "fetch-federal-register-documents": "formal-record",
    "fetch-epa-eis-records": "formal-record",
    "fetch-usbr-project-records": "formal-record",
}
NONPRODUCTIVE_STATUSES = {"failed", "blocked", "receipt-only"}
EXECUTED_STATUSES = {"executed", "fetched", "normalized"}
LOW_VOLUME_THRESHOLD = 3


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


def load_blueprint_themes(run_dir: Path, round_id: str) -> list[dict[str, Any]]:
    artifact = load_json(run_dir / "reporting" / f"report_blueprint_{round_id}.json")
    themes = [item for item in list_items(artifact.get("investigation_themes")) if isinstance(item, dict)]
    if themes:
        return themes
    try:
        payload = query_council_objects(
            run_dir,
            object_kind="investigation-theme",
            round_id=round_id,
            limit=100,
        )
    except Exception:
        return []
    return [item for item in list_items(payload.get("objects")) if isinstance(item, dict)]


def query_source_attempts(run_dir: Path, *, run_id: str, round_id: str) -> list[dict[str, Any]]:
    try:
        payload = query_council_objects(
            run_dir,
            object_kind="source-acquisition-proposal",
            run_id=run_id,
            round_id=round_id,
            limit=500,
        )
    except Exception:
        return []
    return [item for item in list_items(payload.get("objects")) if isinstance(item, dict)]


def signal_rows(run_dir: Path, *, run_id: str, round_id: str) -> list[dict[str, Any]]:
    db_path = run_dir / "analytics" / "signal_plane.sqlite"
    if not db_path.exists():
        return []
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        present = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'normalized_signals'"
        ).fetchone()
        if present is None:
            return []
        rows = connection.execute(
            """
            SELECT signal_id, plane, source_skill, query_text, metric, metadata_json
            FROM normalized_signals
            WHERE run_id = ? AND round_id = ?
            """,
            (run_id, round_id),
        ).fetchall()
    finally:
        connection.close()
    results: list[dict[str, Any]] = []
    for row in rows:
        try:
            metadata = json.loads(maybe_text(row["metadata_json"]) or "{}")
        except json.JSONDecodeError:
            metadata = {}
        results.append(
            {
                "signal_id": maybe_text(row["signal_id"]),
                "plane": maybe_text(row["plane"]),
                "source_skill": maybe_text(row["source_skill"]),
                "query_text": maybe_text(row["query_text"]),
                "metric": maybe_text(row["metric"]),
                "metadata": metadata if isinstance(metadata, dict) else {},
            }
        )
    return results


def source_family_for(row: dict[str, Any]) -> str:
    source_skill = maybe_text(row.get("source_skill"))
    if source_skill in PUBLIC_FAMILY_BY_SKILL:
        return PUBLIC_FAMILY_BY_SKILL[source_skill]
    plane = maybe_text(row.get("plane"))
    if plane == "environment":
        return "environment-observation"
    if plane == "formal":
        return "formal-record"
    if plane == "public":
        return "public-discourse"
    return "other"


def query_variant_hits(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: Counter[tuple[str, str, str]] = Counter()
    for signal in signals:
        query = maybe_text(signal.get("query_text")) or maybe_text(dict_items(signal.get("metadata")).get("query"))
        if not query:
            continue
        counts[(source_family_for(signal), maybe_text(signal.get("source_skill")), query)] += 1
    return [
        {
            "source_family": family,
            "source_skill": source_skill,
            "query_text": query,
            "hit_count": count,
        }
        for (family, source_skill, query), count in sorted(counts.items())
    ]


def attempt_rows(attempts: list[dict[str, Any]], signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    signal_counts = Counter(maybe_text(signal.get("source_skill")) for signal in signals)
    rows: list[dict[str, Any]] = []
    for attempt in attempts:
        source_skill = maybe_text(attempt.get("source_skill"))
        if not source_skill:
            continue
        count = int(signal_counts.get(source_skill, 0))
        status = maybe_text(attempt.get("status")) or "proposed"
        if status in NONPRODUCTIVE_STATUSES:
            attempt_kind = status
        elif status in EXECUTED_STATUSES and count == 0:
            attempt_kind = "zero-result"
        elif 0 < count < LOW_VOLUME_THRESHOLD:
            attempt_kind = "low-volume"
        else:
            attempt_kind = "observed"
        if attempt_kind == "observed":
            continue
        rows.append(
            {
                "proposal_id": maybe_text(attempt.get("proposal_id")) or maybe_text(attempt.get("object_id")),
                "source_skill": source_skill,
                "source_family": PUBLIC_FAMILY_BY_SKILL.get(source_skill, source_family_for({"source_skill": source_skill})),
                "status": status,
                "attempt_kind": attempt_kind,
                "normalized_signal_count": count,
                "query_parameters": dict_items(attempt.get("query_parameters")),
                "evidence_refs": list_items(attempt.get("evidence_refs")),
                "claim_strength_effect": "requires recovery, source-limit rationale, or report downgrade before negative/absence wording",
            }
        )
    return rows


def source_family_counts(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: Counter[str] = Counter(source_family_for(signal) for signal in signals)
    return [
        {"source_family": family, "signal_count": count}
        for family, count in sorted(counts.items())
    ]


def visible_denominators_from_artifacts(run_dir: Path, round_id: str, signals: list[dict[str, Any]]) -> dict[str, Any]:
    coverage = load_json(run_dir / "analytics" / f"public_discourse_coverage_audit_{round_id}.json")
    corpus = load_json(run_dir / "analytics" / f"public_discourse_corpus_{round_id}.json")
    denominators = list_items(coverage.get("source_family_denominators")) or list_items(corpus.get("source_family_denominators"))
    return {
        "source_family_denominators": denominators,
        "signal_source_family_counts": source_family_counts(signals),
        "denominator_policy": dict_items(coverage.get("denominator_policy")) or dict_items(corpus.get("denominator_policy")),
    }


def action_card_risks(run_dir: Path, round_id: str) -> list[str]:
    cards = load_json(run_dir / "analytics" / f"claim_gap_action_cards_{round_id}.json")
    risks: list[str] = []
    for card in list_items(cards.get("action_cards")):
        if not isinstance(card, dict):
            continue
        text = maybe_text(card.get("claim_gap"))
        boundary = maybe_text(card.get("if_not_done_report_boundary"))
        if text or boundary:
            risks.append(maybe_text(f"{text} {boundary}"))
    return unique_texts(risks)[:12]


def checkpoint_for_theme(
    *,
    theme: dict[str, Any],
    run_dir: Path,
    run_id: str,
    round_id: str,
    output_file: Path,
    signals: list[dict[str, Any]],
    attempts: list[dict[str, Any]],
    theme_index: int,
    action_risks: list[str],
) -> dict[str, Any] | None:
    theme_id = maybe_text(theme.get("theme_id")) or f"theme-{theme_index:03d}"
    theme_text = " ".join(
        [
            maybe_text(theme.get("theme_question")),
            maybe_text(theme.get("claim_boundary")),
            " ".join(unique_texts(list_items(theme.get("claim_slots_supported")))),
        ]
    ).casefold()
    family_counts = source_family_counts(signals)
    attempts_for_checkpoint = attempt_rows(attempts, signals)
    risks = list(action_risks)
    recovery: list[str] = []
    relevant_signals = signals
    if "public" in theme_text or "semantic" in theme_text or "media" in theme_text:
        relevant_signals = [signal for signal in signals if maybe_text(signal.get("plane")) in {"public", "formal"}]
        if not relevant_signals:
            risks.append("Public/formal semantic theme has no visible corpus rows; public semantic claims must downgrade to missing basis or examples only.")
            recovery.append("source owner should revise terms, materialize corpus/coverage, or record source-limit rationale")
    if "official" in theme_text or "policy" in theme_text or "governance" in theme_text:
        formal_signals = [signal for signal in signals if maybe_text(signal.get("plane")) == "formal"]
        if not formal_signals:
            risks.append("Official/policy theme has no visible formal or governance record rows; policy response/effectiveness wording must be blocked or downgraded.")
            recovery.append("source owner should seek official action/governance records or document policy-lane absence")
    if "interaction" in theme_text or "timeline" in theme_text:
        timeline = load_json(output_file.parent / f"fact_policy_public_interaction_timeline_{round_id}.json")
        if not list_items(timeline.get("interaction_nodes")):
            risks.append("Interaction theme has no carried interaction nodes; do not write interaction or response-attribution claims.")
            recovery.append("build lane episode cards and interaction timeline, or keep lanes separate")
    if attempts_for_checkpoint:
        recovery.append("review failed, zero, low-volume, or receipt-only attempts before negative wording")
    risk_text = unique_texts(risks)
    recovery_choices = unique_texts(recovery)
    if not risk_text and not attempts_for_checkpoint:
        return None
    checkpoint = {
        "checkpoint_id": "acquisition-checkpoint-" + stable_hash(run_id, round_id, theme_id, len(risk_text), len(attempts_for_checkpoint))[:12],
        "run_id": run_id,
        "round_id": round_id,
        "generated_at_utc": utc_now_iso(),
        "decision_source": "approved-helper-view",
        "theme_id": theme_id,
        "checkpoint_status": "claim-impact-visible",
        "source_family_counts": family_counts,
        "query_variant_hits": query_variant_hits(relevant_signals),
        "zero_low_volume_or_failed_attempts": attempts_for_checkpoint,
        "visible_denominators": visible_denominators_from_artifacts(run_dir, round_id, signals),
        "coverage_risks": risk_text,
        "challenger_quick_review": [
            "Check denominator mixing, GDELT tone misuse, platform bias, policy-lane absence, and unsupported absence wording before report use."
        ],
        "next_recovery_choice": recovery_choices,
        "stop_or_continue_reason": (
            "Checkpoint exists because current acquisition state can change claim strength, source-limit rationale, report downgrade, or recovery choice."
        ),
        "evidence_refs": unique_values([ref for attempt in attempts_for_checkpoint for ref in list_items(attempt.get("evidence_refs"))]),
        "lineage": unique_texts([theme_id, *[maybe_text(attempt.get("proposal_id")) for attempt in attempts_for_checkpoint]]),
        "provenance": {
            "source_skill": SKILL_NAME,
            "decision_source": "approved-helper-view",
            "artifact_path": str(output_file),
        },
    }
    return validate_canonical_payload("acquisition-checkpoint", checkpoint)


def materialize_checkpoints(args: argparse.Namespace) -> dict[str, Any]:
    run_dir = resolve_run_dir(args.run_dir)
    output_file = resolve_path(run_dir, args.output_path, f"analytics/acquisition_checkpoints_{args.round_id}.json")
    signals = signal_rows(run_dir, run_id=args.run_id, round_id=args.round_id)
    attempts = query_source_attempts(run_dir, run_id=args.run_id, round_id=args.round_id)
    themes = load_blueprint_themes(run_dir, args.round_id)
    if maybe_text(args.theme_id):
        themes = [theme for theme in themes if maybe_text(theme.get("theme_id")) == maybe_text(args.theme_id)]
    if not themes:
        themes = [
            {
                "theme_id": "theme-round-acquisition",
                "theme_question": "Round-level acquisition state that may affect report claim strength.",
                "claim_slots_supported": [],
            }
        ]
    risks = action_card_risks(run_dir, args.round_id)
    checkpoints: list[dict[str, Any]] = []
    for index, theme in enumerate(themes, start=1):
        checkpoint = checkpoint_for_theme(
            theme=theme,
            run_dir=run_dir,
            run_id=args.run_id,
            round_id=args.round_id,
            output_file=output_file,
            signals=signals,
            attempts=attempts,
            theme_index=index,
            action_risks=risks,
        )
        if checkpoint:
            checkpoints.append(checkpoint)
    wrapper = {
        "schema_version": "acquisition-checkpoint-materialization-v1",
        "skill": SKILL_NAME,
        "run_id": args.run_id,
        "round_id": args.round_id,
        "generated_at_utc": utc_now_iso(),
        "status": "completed",
        "acquisition_checkpoints": checkpoints,
        "checkpoint_count": len(checkpoints),
        "checkpoint_policy": {
            "only_when_claim_impact_visible": True,
            "not_per_tool_call_form": True,
            "does_not_replace_finding_or_sufficiency_review": True,
        },
        "source_parameters": {"signal_scope": "run_id+round_id"},
        "query_parameters": {"theme_id": maybe_text(args.theme_id)},
        "provenance": {"source_skill": SKILL_NAME, "decision_source": "approved-helper-view"},
        "artifact_refs": [
            {
                "artifact_path": str(output_file),
                "record_locator": "$.acquisition_checkpoints",
                "artifact_ref": f"{output_file}:$.acquisition_checkpoints",
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
            "checkpoint_count": len(checkpoints),
        },
        "receipt_id": "acquisition-checkpoint-receipt-" + stable_hash(args.run_id, args.round_id, output_file)[:20],
        "artifact_refs": wrapper["artifact_refs"],
        "canonical_ids": [maybe_text(item.get("checkpoint_id")) for item in checkpoints],
        "warnings": [],
        "board_handoff": {
            "candidate_ids": [maybe_text(item.get("checkpoint_id")) for item in checkpoints],
            "gap_hints": [risk for item in checkpoints for risk in list_items(item.get("coverage_risks"))],
            "suggested_next_skills": [],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize claim-impact acquisition checkpoints.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--theme-id", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = materialize_checkpoints(args)
    sys.stdout.write(pretty_json(payload, args.pretty) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
