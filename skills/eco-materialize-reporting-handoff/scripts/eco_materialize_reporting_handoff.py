#!/usr/bin/env python3
"""Materialize a compact reporting handoff from promotion-stage artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKILL_NAME = "eco-materialize-reporting-handoff"


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


def excerpt_text(text: str, limit: int = 280) -> str:
    normalized = maybe_text(text)
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3].rstrip() + "..."


def build_key_findings(selected_coverages: list[dict[str, Any]], max_findings: int) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for index, coverage in enumerate(selected_coverages[: max(1, max_findings)], start=1):
        if not isinstance(coverage, dict):
            continue
        claim_id = maybe_text(coverage.get("claim_id"))
        coverage_id = maybe_text(coverage.get("coverage_id"))
        readiness = maybe_text(coverage.get("readiness")) or "unknown"
        coverage_score = maybe_number(coverage.get("coverage_score")) or 0.0
        support_count = int(coverage.get("support_link_count") or 0)
        contradiction_count = int(coverage.get("contradiction_link_count") or 0)
        summary = (
            f"Claim {claim_id or coverage_id or index} is {readiness} with coverage_score={coverage_score:.2f}, "
            f"support_links={support_count}, contradiction_links={contradiction_count}."
        )
        findings.append(
            {
                "finding_id": f"reporting-finding-{index:03d}",
                "title": f"Evidence basis for {claim_id or coverage_id or 'round target'}",
                "summary": summary,
                "coverage_id": coverage_id,
                "claim_id": claim_id,
                "readiness": readiness,
                "coverage_score": coverage_score,
                "evidence_refs": unique_texts(coverage.get("evidence_refs", []) if isinstance(coverage.get("evidence_refs"), list) else []),
            }
        )
    return findings


def build_open_risks(
    *,
    promotion_basis: dict[str, Any],
    supervisor_state: dict[str, Any],
    readiness: dict[str, Any],
) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    remaining_risks = promotion_basis.get("remaining_risks", []) if isinstance(promotion_basis.get("remaining_risks"), list) else []
    for index, risk in enumerate(remaining_risks, start=1):
        if not isinstance(risk, dict):
            continue
        results.append(
            {
                "risk_id": maybe_text(risk.get("action_id")) or f"promotion-risk-{index:03d}",
                "risk_type": maybe_text(risk.get("action_kind")) or "investigation",
                "priority": maybe_text(risk.get("priority")) or "medium",
                "summary": maybe_text(risk.get("reason")) or "Promotion basis still carries unresolved risk.",
            }
        )
    operator_notes = supervisor_state.get("operator_notes", []) if isinstance(supervisor_state.get("operator_notes"), list) else []
    for index, note in enumerate(operator_notes, start=1):
        text = maybe_text(note)
        if not text:
            continue
        results.append(
            {
                "risk_id": f"operator-note-{index:03d}",
                "risk_type": "operator-note",
                "priority": "medium",
                "summary": text,
            }
        )
    gate_reasons = readiness.get("gate_reasons", []) if isinstance(readiness.get("gate_reasons"), list) else []
    for index, reason in enumerate(gate_reasons, start=1):
        text = maybe_text(reason)
        if not text:
            continue
        results.append(
            {
                "risk_id": f"gate-reason-{index:03d}",
                "risk_type": "gate",
                "priority": "high",
                "summary": text,
            }
        )
    deduped: dict[str, dict[str, str]] = {}
    for item in results:
        key = "|".join([item.get("risk_type", ""), item.get("summary", "")])
        deduped.setdefault(key, item)
    return list(deduped.values())[:6]


def build_recommended_next_actions(supervisor_state: dict[str, Any]) -> list[dict[str, str]]:
    top_actions = supervisor_state.get("top_actions", []) if isinstance(supervisor_state.get("top_actions"), list) else []
    recommendations: list[dict[str, str]] = []
    for action in top_actions[:4]:
        if not isinstance(action, dict):
            continue
        objective = maybe_text(action.get("objective"))
        if not objective:
            continue
        assigned_role = maybe_text(action.get("assigned_role")) or "unspecified"
        action_kind = maybe_text(action.get("action_kind")) or "follow-up"
        priority = maybe_text(action.get("priority")) or "medium"
        recommendations.append(
            {
                "assigned_role": assigned_role,
                "objective": objective,
                "reason": f"Supervisor ranked this as the next {action_kind} follow-up (priority={priority}).",
            }
        )
    return recommendations


def recommended_sections(handoff_status: str) -> list[str]:
    if handoff_status == "ready-for-reporting":
        return ["executive-summary", "role-reports", "evidence-basis", "residual-risks", "audit-trace"]
    return ["gating-status", "open-risks", "next-round-plan", "audit-trace"]


def materialize_reporting_handoff_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    promotion_path: str,
    readiness_path: str,
    board_brief_path: str,
    supervisor_state_path: str,
    output_path: str,
    max_findings: int,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    promotion_file = resolve_path(run_dir_path, promotion_path, f"promotion/promoted_evidence_basis_{round_id}.json")
    readiness_file = resolve_path(run_dir_path, readiness_path, f"reporting/round_readiness_{round_id}.json")
    board_brief_file = resolve_path(run_dir_path, board_brief_path, f"board/board_brief_{round_id}.md")
    supervisor_file = resolve_path(run_dir_path, supervisor_state_path, f"runtime/supervisor_state_{round_id}.json")
    output_file = resolve_path(run_dir_path, output_path, f"reporting/reporting_handoff_{round_id}.json")

    warnings: list[dict[str, Any]] = []
    promotion_basis = load_json_if_exists(promotion_file)
    if not isinstance(promotion_basis, dict):
        warnings.append({"code": "missing-promotion-basis", "message": f"No promotion basis artifact was found at {promotion_file}."})
        promotion_basis = {"promotion_status": "withheld", "selected_coverages": [], "selected_evidence_refs": [], "remaining_risks": []}
    readiness = load_json_if_exists(readiness_file)
    if not isinstance(readiness, dict):
        warnings.append({"code": "missing-readiness", "message": f"No round readiness artifact was found at {readiness_file}."})
        readiness = {"readiness_status": "blocked", "gate_reasons": []}
    supervisor_state = load_json_if_exists(supervisor_file)
    if not isinstance(supervisor_state, dict):
        supervisor_state = {"supervisor_status": "unavailable", "top_actions": [], "operator_notes": []}
    board_brief_text = load_text_if_exists(board_brief_file)

    promotion_status = maybe_text(promotion_basis.get("promotion_status")) or "withheld"
    readiness_status = maybe_text(readiness.get("readiness_status")) or "blocked"
    supervisor_status = maybe_text(supervisor_state.get("supervisor_status")) or "unavailable"
    handoff_status = "ready-for-reporting" if promotion_status == "promoted" else "pending-more-investigation"

    selected_coverages = promotion_basis.get("selected_coverages", []) if isinstance(promotion_basis.get("selected_coverages"), list) else []
    key_findings = build_key_findings(selected_coverages, max_findings)
    open_risks = build_open_risks(promotion_basis=promotion_basis, supervisor_state=supervisor_state, readiness=readiness)
    next_actions = build_recommended_next_actions(supervisor_state)
    board_excerpt = excerpt_text(board_brief_text)
    handoff_id = "reporting-handoff-" + stable_hash(run_id, round_id, handoff_status, promotion_status)[:12]

    wrapper = {
        "schema_version": "e1.0",
        "skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "handoff_id": handoff_id,
        "handoff_status": handoff_status,
        "promotion_status": promotion_status,
        "readiness_status": readiness_status,
        "supervisor_status": supervisor_status,
        "promotion_path": str(promotion_file),
        "readiness_path": str(readiness_file),
        "board_brief_path": str(board_brief_file),
        "supervisor_state_path": str(supervisor_file),
        "promoted_basis_id": maybe_text(promotion_basis.get("basis_id")),
        "selected_evidence_refs": unique_texts(promotion_basis.get("selected_evidence_refs", []) if isinstance(promotion_basis.get("selected_evidence_refs"), list) else []),
        "board_brief_excerpt": board_excerpt,
        "key_findings": key_findings,
        "open_risks": open_risks,
        "recommended_next_actions": next_actions,
        "recommended_sections": recommended_sections(handoff_status),
        "report_targets": ["expert-report-draft", "council-decision-draft"] if handoff_status == "ready-for-reporting" else ["expert-report-draft", "another-round-decision"],
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
            "handoff_id": handoff_id,
            "handoff_status": handoff_status,
            "finding_count": len(key_findings),
        },
        "receipt_id": "reporting-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, handoff_id)[:20],
        "batch_id": "reportingbatch-" + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [handoff_id],
        "warnings": warnings,
        "board_handoff": {
            "candidate_ids": [handoff_id],
            "evidence_refs": artifact_refs,
            "gap_hints": [item.get("summary", "") for item in open_risks[:3] if maybe_text(item.get("summary"))] if handoff_status != "ready-for-reporting" else [],
            "challenge_hints": [item.get("summary", "") for item in open_risks[:2] if maybe_text(item.get("summary"))],
            "suggested_next_skills": ["eco-draft-expert-report", "eco-draft-council-decision"] if handoff_status == "ready-for-reporting" else ["eco-draft-expert-report", "eco-draft-council-decision", "eco-propose-next-actions", "eco-open-falsification-probe"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize a compact reporting handoff from promotion-stage artifacts.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--promotion-path", default="")
    parser.add_argument("--readiness-path", default="")
    parser.add_argument("--board-brief-path", default="")
    parser.add_argument("--supervisor-state-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--max-findings", type=int, default=3)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = materialize_reporting_handoff_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        promotion_path=args.promotion_path,
        readiness_path=args.readiness_path,
        board_brief_path=args.board_brief_path,
        supervisor_state_path=args.supervisor_state_path,
        output_path=args.output_path,
        max_findings=args.max_findings,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())