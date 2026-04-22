from __future__ import annotations

from pathlib import Path
from typing import Any

from .phase2_promotion_resolution import (
    load_council_proposals,
    load_council_readiness_opinions,
    resolve_promotion_council_inputs,
)
from .kernel.deliberation_plane import store_promotion_freeze_record
from .kernel.manifest import write_json
from .kernel.paths import promotion_gate_path
from .kernel.phase2_state_surfaces import load_round_readiness_wrapper


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


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


def utc_now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_path(run_dir: Path, override: str, default_path: Path) -> Path:
    text = maybe_text(override)
    if not text:
        return default_path.resolve()
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = run_dir / candidate
    return candidate.resolve()


def apply_promotion_gate(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    readiness_path_override: str = "",
    output_path_override: str = "",
) -> dict[str, Any]:
    readiness_file = resolve_path(
        run_dir,
        readiness_path_override,
        run_dir / "reporting" / f"round_readiness_{round_id}.json",
    )
    output_file = resolve_path(
        run_dir,
        output_path_override,
        promotion_gate_path(run_dir, round_id),
    )

    readiness_context = load_round_readiness_wrapper(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        readiness_path=readiness_path_override,
    )
    readiness = (
        readiness_context.get("payload")
        if isinstance(readiness_context.get("payload"), dict)
        else {}
    )
    readiness_status = maybe_text(readiness.get("readiness_status")) or "blocked"
    gate_reasons = (
        readiness.get("gate_reasons", [])
        if isinstance(readiness.get("gate_reasons"), list)
        else []
    )
    recommended_next_skills = (
        readiness.get("recommended_next_skills", [])
        if isinstance(readiness.get("recommended_next_skills"), list)
        else []
    )
    warnings: list[dict[str, str]] = []
    if not readiness:
        warnings.append(
            {
                "code": "missing-readiness",
                "message": (
                    "No round readiness artifact or DB assessment was found at "
                    f"{readiness_file}."
                ),
            }
        )
    council_proposals = load_council_proposals(
        run_dir,
        run_id=run_id,
        round_id=round_id,
    )
    council_opinions = load_council_readiness_opinions(
        run_dir,
        run_id=run_id,
        round_id=round_id,
    )
    promotion_resolution = resolve_promotion_council_inputs(
        council_proposals,
        council_opinions,
        readiness_status=readiness_status,
        allow_non_ready=False,
        round_id=round_id,
    )
    promote_allowed = bool(promotion_resolution.get("promote_allowed"))
    gate_status = maybe_text(promotion_resolution.get("gate_status")) or (
        "allow-promote" if promote_allowed else "freeze-withheld"
    )
    promotion_resolution_reasons = (
        promotion_resolution.get("promotion_resolution_reasons", [])
        if isinstance(promotion_resolution.get("promotion_resolution_reasons"), list)
        else []
    )
    gate_reasons = unique_texts(
        [
            *[
                maybe_text(item)
                for item in promotion_resolution_reasons
                if maybe_text(item)
            ],
            *[maybe_text(item) for item in gate_reasons if maybe_text(item)],
        ]
    )

    payload = {
        "schema_version": "runtime-gate-v1",
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "stage_name": "promotion-gate",
        "gate_handler": "promotion-gate",
        "readiness_path": str(readiness_file),
        "readiness_status": readiness_status,
        "promote_allowed": promote_allowed,
        "gate_status": gate_status,
        "decision_source": maybe_text(promotion_resolution.get("decision_source"))
        or maybe_text(readiness.get("decision_source"))
        or "policy-fallback",
        "promotion_resolution_mode": maybe_text(
            promotion_resolution.get("promotion_resolution_mode")
        ),
        "gate_reasons": [
            maybe_text(item) for item in gate_reasons if maybe_text(item)
        ],
        "supporting_proposal_ids": (
            promotion_resolution.get("supporting_proposal_ids", [])
            if isinstance(promotion_resolution.get("supporting_proposal_ids"), list)
            else []
        ),
        "rejected_proposal_ids": (
            promotion_resolution.get("rejected_proposal_ids", [])
            if isinstance(promotion_resolution.get("rejected_proposal_ids"), list)
            else []
        ),
        "supporting_opinion_ids": (
            promotion_resolution.get("supporting_opinion_ids", [])
            if isinstance(promotion_resolution.get("supporting_opinion_ids"), list)
            else []
        ),
        "rejected_opinion_ids": (
            promotion_resolution.get("rejected_opinion_ids", [])
            if isinstance(promotion_resolution.get("rejected_opinion_ids"), list)
            else []
        ),
        "council_input_counts": (
            promotion_resolution.get("council_input_counts", {})
            if isinstance(promotion_resolution.get("council_input_counts"), dict)
            else {}
        ),
        "recommended_next_skills": [
            maybe_text(item)
            for item in recommended_next_skills
            if maybe_text(item)
        ],
        "warnings": warnings,
    }
    write_json(output_file, payload)
    payload["output_path"] = str(output_file)
    store_promotion_freeze_record(
        run_dir,
        run_id=run_id,
        round_id=round_id,
        gate_snapshot=payload,
        artifact_paths={"promotion_gate_path": str(output_file)},
    )
    return payload
