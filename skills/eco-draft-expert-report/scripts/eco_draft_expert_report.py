#!/usr/bin/env python3
"""Draft a role-specific expert report from reporting handoff artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "eco-draft-expert-report"
ROLE_VALUES = ("sociologist", "environmentalist")
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.reporting_contracts import (  # noqa: E402
    reporting_contract_fields_from_payload,
)
from eco_council_runtime.kernel.deliberation_plane import (  # noqa: E402
    store_expert_report_record,
)
from eco_council_runtime.kernel.phase2_state_surfaces import (  # noqa: E402
    load_council_decision_wrapper,
    load_reporting_handoff_wrapper,
)
from eco_council_runtime.reporting_status import (  # noqa: E402
    reporting_gate_state,
)


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


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


def role_profile(role: str) -> dict[str, Any]:
    if role == "sociologist":
        return {
            "label": "public-discussion",
            "summary_prefix": "Public-facing interpretation",
            "section_hints": ["role-summary", "public-claim-interpretation", "evidence-basis", "residual-risks"],
        }
    return {
        "label": "physical-evidence",
        "summary_prefix": "Physical evidence interpretation",
        "section_hints": ["role-summary", "physical-evidence", "coverage-posture", "residual-risks"],
    }


def role_findings(role: str, key_findings: list[dict[str, Any]], max_findings: int, board_excerpt: str) -> list[dict[str, Any]]:
    profile = role_profile(role)
    findings: list[dict[str, Any]] = []
    for index, finding in enumerate(key_findings[: max(1, max_findings)], start=1):
        if not isinstance(finding, dict):
            continue
        title = maybe_text(finding.get("title")) or f"{profile['summary_prefix']} {index}"
        summary = maybe_text(finding.get("summary"))
        if role == "sociologist" and board_excerpt:
            summary = f"{summary} Board context: {board_excerpt}".strip()
        findings.append(
            {
                "finding_id": maybe_text(finding.get("finding_id")) or f"expert-finding-{index:03d}",
                "title": title,
                "summary": summary,
                "focus": profile["label"],
                "claim_id": maybe_text(finding.get("claim_id")),
                "coverage_id": maybe_text(finding.get("coverage_id")),
                "evidence_refs": unique_texts(finding.get("evidence_refs", []) if isinstance(finding.get("evidence_refs"), list) else []),
            }
        )
    return findings


def role_questions(role: str, open_risks: list[dict[str, Any]], decision_summary: str) -> list[str]:
    questions: list[str] = []
    for risk in open_risks[:4]:
        if not isinstance(risk, dict):
            continue
        summary = maybe_text(risk.get("summary"))
        if not summary:
            continue
        questions.append(f"As {role}, how should this unresolved issue be addressed: {summary}?")
    if not questions and maybe_text(decision_summary):
        questions.append(f"As {role}, how should the report explain: {maybe_text(decision_summary)}?")
    return unique_texts(questions)[:4]


def filtered_actions(role: str, recommendations: list[dict[str, Any]], report_status: str) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    for item in recommendations:
        if not isinstance(item, dict):
            continue
        assigned_role = maybe_text(item.get("assigned_role"))
        if assigned_role and assigned_role != role:
            continue
        objective = maybe_text(item.get("objective"))
        if not objective:
            continue
        results.append(
            {
                "assigned_role": role,
                "objective": objective,
                "reason": maybe_text(item.get("reason")) or "Carry this role-specific follow-up into the next round.",
            }
        )
    if results:
        return results[:4]
    if report_status == "ready-to-publish":
        return [
            {
                "assigned_role": role,
                "objective": f"Finalize the {role} report around the promoted evidence basis.",
                "reason": "The round is ready for publication and this role can now consolidate its narrative.",
            }
        ]
    return []


def draft_expert_report_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    role: str,
    reporting_handoff_path: str,
    decision_path: str,
    board_brief_path: str,
    output_path: str,
    max_findings: int,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    handoff_file = resolve_path(run_dir_path, reporting_handoff_path, f"reporting/reporting_handoff_{round_id}.json")
    decision_file = resolve_path(run_dir_path, decision_path, f"reporting/council_decision_draft_{round_id}.json")
    board_brief_file = resolve_path(run_dir_path, board_brief_path, f"board/board_brief_{round_id}.md")
    output_file = resolve_path(run_dir_path, output_path, f"reporting/expert_report_draft_{role}_{round_id}.json")

    warnings: list[dict[str, Any]] = []
    handoff_context = load_reporting_handoff_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        reporting_handoff_path=reporting_handoff_path,
    )
    handoff_payload = (
        handoff_context.get("payload")
        if isinstance(handoff_context.get("payload"), dict)
        else None
    )
    if not isinstance(handoff_payload, dict):
        missing_message = (
            "No reporting handoff DB record was found for "
            f"{handoff_file}; artifact exists but is orphaned from the reporting plane."
            if bool(handoff_context.get("artifact_present"))
            else (
                "No reporting handoff artifact or DB record was found "
                f"at {handoff_file}."
            )
        )
        warnings.append(
            {
                "code": "missing-reporting-handoff",
                "message": missing_message,
            }
        )
        handoff = {
            "handoff_status": "investigation-open",
            "reporting_ready": False,
            "reporting_blockers": ["reporting-handoff-missing"],
            "promotion_status": "withheld",
            "key_findings": [],
            "open_risks": [],
            "recommended_next_actions": [],
        }
    else:
        handoff = handoff_payload
    decision_context = load_council_decision_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        decision_stage="draft",
        decision_path=decision_path,
    )
    decision_payload = (
        decision_context.get("payload")
        if isinstance(decision_context.get("payload"), dict)
        else None
    )
    decision = decision_payload or {}
    board_excerpt = maybe_text(load_text_if_exists(board_brief_file))[:220]
    contract_fields = reporting_contract_fields_from_payload(
        decision_payload,
        fallback_payload=handoff_payload,
        observed_inputs_overrides={
            "reporting_handoff_artifact_present": bool(
                handoff_context.get("artifact_present")
            ),
            "reporting_handoff_present": bool(handoff_context.get("payload_present")),
            "decision_artifact_present": bool(
                decision_context.get("artifact_present")
            ),
            "decision_present": bool(decision_context.get("payload_present")),
            "board_brief_artifact_present": board_brief_file.exists(),
            "board_brief_present": bool(board_excerpt),
        },
        field_overrides={
            "reporting_handoff_source": maybe_text(handoff_context.get("source"))
            or "missing-reporting-handoff",
            "decision_source": maybe_text(decision_context.get("source"))
            or "missing-decision",
            "board_brief_source": (
                "board-brief-artifact"
                if board_brief_file.exists()
                else "missing-board-brief"
            ),
        },
    )

    gate_state = reporting_gate_state(
        promotion_status=maybe_text(handoff.get("promotion_status"))
        or maybe_text(decision.get("promotion_status"))
        or "withheld",
        readiness_status=maybe_text(handoff.get("readiness_status")) or "blocked",
        supervisor_status=maybe_text(handoff.get("supervisor_status")) or "unavailable",
        require_supervisor=True,
        reporting_ready=decision.get("reporting_ready")
        if "reporting_ready" in decision
        else handoff.get("reporting_ready"),
        reporting_blockers_value=decision.get("reporting_blockers")
        if isinstance(decision.get("reporting_blockers"), list)
        else handoff.get("reporting_blockers"),
        handoff_status=maybe_text(handoff.get("handoff_status")),
    )
    handoff_status = maybe_text(gate_state.get("handoff_status")) or "investigation-open"
    reporting_ready = bool(gate_state.get("reporting_ready"))
    reporting_blockers = unique_texts(
        gate_state.get("reporting_blockers", [])
        if isinstance(gate_state.get("reporting_blockers"), list)
        else []
    )
    publication_readiness = maybe_text(decision.get("publication_readiness")) or ("ready" if reporting_ready else "hold")
    report_status = "ready-to-publish" if reporting_ready and publication_readiness == "ready" else "needs-more-evidence"
    key_findings = handoff.get("key_findings", []) if isinstance(handoff.get("key_findings"), list) else []
    open_risks = handoff.get("open_risks", []) if isinstance(handoff.get("open_risks"), list) else []
    recommendations = handoff.get("recommended_next_actions", []) if isinstance(handoff.get("recommended_next_actions"), list) else []
    decision_summary = maybe_text(decision.get("decision_summary"))
    profile = role_profile(role)
    report_id = f"expert-report-{role}-{round_id}"

    payload = {
        "schema_version": "e1.1",
        "skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "report_id": report_id,
        "agent_role": role,
        "status": report_status,
        "handoff_status": handoff_status,
        "reporting_ready": reporting_ready,
        "reporting_blockers": reporting_blockers,
        "publication_readiness": publication_readiness,
        **contract_fields,
        "summary": (
            f"{profile['summary_prefix']} for round {round_id}. {decision_summary or 'The round still needs more reporting context.'}"
            if report_status == "ready-to-publish"
            else f"{profile['summary_prefix']} for round {round_id}. Another investigation pass is still required before final publication."
        ),
        "findings": role_findings(role, key_findings, max_findings, board_excerpt),
        "open_questions": role_questions(role, open_risks, decision_summary),
        "recommended_next_actions": filtered_actions(role, recommendations, report_status),
        "report_sections": profile["section_hints"],
        "audit_refs": {
            "reporting_handoff_path": str(handoff_file),
            "decision_path": str(decision_file),
            "board_brief_path": str(board_brief_file),
        },
        "selected_evidence_refs": unique_texts(handoff.get("selected_evidence_refs", []) if isinstance(handoff.get("selected_evidence_refs"), list) else []),
    }
    stored_payload = store_expert_report_record(
        run_dir_path,
        report_payload=payload,
        artifact_path=str(output_file),
    )
    report_id = maybe_text(stored_payload.get("report_id")) or report_id
    write_json_file(output_file, stored_payload)

    artifact_refs = [{"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"}]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "role": role,
            "output_path": str(output_file),
            "report_id": report_id,
            "report_status": report_status,
            "board_state_source": contract_fields["board_state_source"],
            "coverage_source": contract_fields["coverage_source"],
            "reporting_handoff_source": maybe_text(contract_fields.get("reporting_handoff_source")),
            "decision_source": maybe_text(contract_fields.get("decision_source")),
            "board_brief_source": maybe_text(contract_fields.get("board_brief_source")),
            "db_path": contract_fields["db_path"],
        },
        "receipt_id": "reporting-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, role, report_id)[:20],
        "batch_id": "reportingbatch-" + stable_hash(SKILL_NAME, run_id, round_id, role, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [report_id],
        "warnings": warnings,
        "deliberation_sync": contract_fields["deliberation_sync"],
        "analysis_sync": contract_fields["analysis_sync"],
        "board_handoff": {
            "candidate_ids": [report_id],
            "evidence_refs": artifact_refs,
            "gap_hints": [maybe_text(item.get("summary")) for item in open_risks[:3] if maybe_text(item.get("summary"))] if report_status != "ready-to-publish" else [],
            "challenge_hints": [maybe_text(item) for item in payload["open_questions"] if maybe_text(item)],
            "suggested_next_skills": ["eco-publish-expert-report", "eco-publish-council-decision"] if report_status == "ready-to-publish" else ["eco-submit-council-proposal", "eco-submit-readiness-opinion", "eco-propose-next-actions", "eco-open-falsification-probe"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Draft a role-specific expert report from the reporting handoff and decision draft.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--role", required=True, choices=ROLE_VALUES)
    parser.add_argument("--reporting-handoff-path", default="")
    parser.add_argument("--decision-path", default="")
    parser.add_argument("--board-brief-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--max-findings", type=int, default=3)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = draft_expert_report_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        role=args.role,
        reporting_handoff_path=args.reporting_handoff_path,
        decision_path=args.decision_path,
        board_brief_path=args.board_brief_path,
        output_path=args.output_path,
        max_findings=args.max_findings,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
