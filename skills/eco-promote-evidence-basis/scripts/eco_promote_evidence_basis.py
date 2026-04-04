#!/usr/bin/env python3
"""Promote round evidence into a compact basis artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "eco-promote-evidence-basis"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.analysis_plane import load_evidence_coverage_context  # noqa: E402
from eco_council_runtime.kernel.reporting_contracts import (  # noqa: E402
    reporting_contract_fields_from_payload,
)


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


def maybe_number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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


def stable_hash(*parts: Any) -> str:
    joined = "||".join(maybe_text(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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


def load_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return payload
    return None


def load_text_if_exists(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def promote_evidence_basis_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    readiness_path: str,
    board_brief_path: str,
    coverage_path: str,
    next_actions_path: str,
    output_path: str,
    allow_non_ready: bool,
    max_coverages: int,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    readiness_file = resolve_path(run_dir_path, readiness_path, f"reporting/round_readiness_{round_id}.json")
    board_brief_file = resolve_path(run_dir_path, board_brief_path, f"board/board_brief_{round_id}.md")
    next_actions_file = resolve_path(run_dir_path, next_actions_path, f"investigation/next_actions_{round_id}.json")
    output_file = resolve_path(run_dir_path, output_path, f"promotion/promoted_evidence_basis_{round_id}.json")

    warnings: list[dict[str, Any]] = []
    readiness_payload = load_json_if_exists(readiness_file)
    if not isinstance(readiness_payload, dict):
        warnings.append({"code": "missing-readiness", "message": f"No round readiness artifact was found at {readiness_file}."})
        readiness = {"readiness_status": "blocked", "gate_reasons": ["Missing round readiness artifact."], "counts": {}, "recommended_next_skills": []}
    else:
        readiness = readiness_payload
    coverage_context = load_evidence_coverage_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        coverage_path=coverage_path,
    )
    coverage_warnings = (
        coverage_context.get("warnings", [])
        if isinstance(coverage_context.get("warnings"), list)
        else []
    )
    warnings.extend(coverage_warnings)
    coverages = (
        coverage_context.get("coverages", [])
        if isinstance(coverage_context.get("coverages"), list)
        else []
    )
    coverage_file = maybe_text(coverage_context.get("coverage_file"))
    coverage_source = maybe_text(coverage_context.get("coverage_source")) or "missing-coverage"
    db_path = maybe_text(coverage_context.get("db_path"))
    analysis_sync = (
        coverage_context.get("analysis_sync")
        if isinstance(coverage_context.get("analysis_sync"), dict)
        else {}
    )
    next_actions_payload = load_json_if_exists(next_actions_file)
    next_actions = next_actions_payload
    if not isinstance(next_actions, dict):
        next_actions = {"ranked_actions": []}
    brief_text = maybe_text(load_text_if_exists(board_brief_file))
    contract_fields = reporting_contract_fields_from_payload(
        readiness_payload,
        observed_inputs_overrides={
            "readiness_artifact_present": readiness_file.exists(),
            "readiness_present": isinstance(readiness_payload, dict),
            "board_brief_artifact_present": board_brief_file.exists(),
            "board_brief_present": bool(brief_text),
            "coverage_artifact_present": bool(
                coverage_context.get("coverage_artifact_present")
            ),
            "coverage_present": bool(coverages),
            "next_actions_artifact_present": next_actions_file.exists(),
            "next_actions_present": isinstance(next_actions_payload, dict),
        },
        field_overrides={
            "coverage_source": coverage_source or "missing-coverage",
            "db_path": db_path,
            "readiness_source": (
                "round-readiness-artifact"
                if readiness_file.exists()
                else "missing-readiness"
            ),
            "board_brief_source": (
                "board-brief-artifact"
                if board_brief_file.exists()
                else "missing-board-brief"
            ),
            "next_actions_source": (
                "next-actions-artifact"
                if next_actions_file.exists()
                else "missing-next-actions"
            ),
        },
    )

    readiness_status = maybe_text(readiness.get("readiness_status")) or "blocked"
    promotion_status = "promoted" if readiness_status == "ready" or allow_non_ready else "withheld"
    if promotion_status == "withheld":
        warnings.append({"code": "promotion-withheld", "message": "Promotion was withheld because the round-readiness gate is not ready."})

    ranked_coverages = sorted(coverages, key=lambda item: (-float(item.get("coverage_score") or 0.0), maybe_text(item.get("coverage_id"))))[: max(1, max_coverages)]
    selected_coverages = [
        {
            "coverage_id": maybe_text(item.get("coverage_id")),
            "claim_id": maybe_text(item.get("claim_id")),
            "coverage_score": float(item.get("coverage_score") or 0.0),
            "readiness": maybe_text(item.get("readiness")),
            "support_link_count": int(item.get("support_link_count") or 0),
            "contradiction_link_count": int(item.get("contradiction_link_count") or 0),
            "evidence_refs": unique_texts(item.get("evidence_refs", []) if isinstance(item.get("evidence_refs"), list) else []),
        }
        for item in ranked_coverages
    ]
    selected_evidence_refs = unique_texts([ref for item in selected_coverages for ref in item.get("evidence_refs", [])])
    remaining_risks = [
        {
            "action_id": maybe_text(item.get("action_id")),
            "action_kind": maybe_text(item.get("action_kind")),
            "priority": maybe_text(item.get("priority")),
            "reason": maybe_text(item.get("reason")),
        }
        for item in next_actions.get("ranked_actions", [])
        if isinstance(item, dict) and maybe_text(item.get("action_kind")) != "prepare-promotion"
    ][:4] if isinstance(next_actions.get("ranked_actions"), list) else []

    basis_id = "evidence-basis-" + stable_hash(run_id, round_id, promotion_status)[:12]
    wrapper = {
        "schema_version": "d2.1",
        "skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "basis_id": basis_id,
        "promotion_status": promotion_status,
        "readiness_status": readiness_status,
        "readiness_path": str(readiness_file),
        "board_brief_path": str(board_brief_file),
        "coverage_path": str(coverage_file),
        **contract_fields,
        "selected_coverages": selected_coverages,
        "selected_evidence_refs": selected_evidence_refs,
        "board_brief_excerpt": brief_text[:300],
        "gate_reasons": readiness.get("gate_reasons", []) if isinstance(readiness.get("gate_reasons"), list) else [],
        "remaining_risks": remaining_risks,
        "promotion_notes": (
            "Round is ready and a compact evidence basis has been frozen for downstream reporting."
            if promotion_status == "promoted"
            else "Round is not yet ready; the basis artifact records the strongest available evidence but remains withheld."
        ),
    }
    write_json_file(output_file, wrapper)

    artifact_refs = [{"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"}]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "basis_id": basis_id,
            "promotion_status": promotion_status,
            "board_state_source": contract_fields["board_state_source"],
            "coverage_source": contract_fields["coverage_source"],
            "readiness_source": maybe_text(contract_fields.get("readiness_source")),
            "board_brief_source": maybe_text(contract_fields.get("board_brief_source")),
            "next_actions_source": maybe_text(contract_fields.get("next_actions_source")),
            "db_path": contract_fields["db_path"],
        },
        "receipt_id": "promotion-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, basis_id)[:20],
        "batch_id": "promotionbatch-" + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [basis_id],
        "warnings": warnings,
        "deliberation_sync": contract_fields["deliberation_sync"],
        "analysis_sync": contract_fields["analysis_sync"],
        "board_handoff": {
            "candidate_ids": unique_texts([basis_id] + [item.get("coverage_id") for item in selected_coverages]),
            "evidence_refs": artifact_refs,
            "gap_hints": [] if promotion_status == "promoted" else [maybe_text(reason) for reason in readiness.get("gate_reasons", [])[:3]] if isinstance(readiness.get("gate_reasons"), list) else ["Round readiness is not yet sufficient for promotion."],
            "challenge_hints": [],
            "suggested_next_skills": ["eco-post-board-note"] if promotion_status == "promoted" else ["eco-summarize-round-readiness", "eco-open-falsification-probe"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Promote round evidence into a compact basis artifact.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--readiness-path", default="")
    parser.add_argument("--board-brief-path", default="")
    parser.add_argument("--coverage-path", default="")
    parser.add_argument("--next-actions-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--allow-non-ready", action="store_true")
    parser.add_argument("--max-coverages", type=int, default=3)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = promote_evidence_basis_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        readiness_path=args.readiness_path,
        board_brief_path=args.board_brief_path,
        coverage_path=args.coverage_path,
        next_actions_path=args.next_actions_path,
        output_path=args.output_path,
        allow_non_ready=args.allow_non_ready,
        max_coverages=args.max_coverages,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
