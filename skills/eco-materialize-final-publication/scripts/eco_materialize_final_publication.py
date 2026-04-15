#!/usr/bin/env python3
"""Materialize a final publication artifact from canonical reporting outputs."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "eco-materialize-final-publication"
ROLE_VALUES = ("sociologist", "environmentalist")
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.reporting_contracts import (  # noqa: E402
    reporting_contract_fields_from_payload,
)
from eco_council_runtime.kernel.investigation_planning import (  # noqa: E402
    load_promotion_basis_wrapper,
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


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def payload_without_generated_at(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key != "generated_at_utc"}


def report_summary(role: str, report_payload: dict[str, Any], path: Path) -> dict[str, Any]:
    findings = report_payload.get("findings", []) if isinstance(report_payload.get("findings"), list) else []
    return {
        "role": role,
        "report_id": maybe_text(report_payload.get("report_id")),
        "status": maybe_text(report_payload.get("status")),
        "summary": maybe_text(report_payload.get("summary")),
        "finding_count": len(findings),
        "report_path": str(path),
    }


def release_summary(*, decision: dict[str, Any], handoff: dict[str, Any], publication_posture: str, report_rows: list[dict[str, Any]]) -> str:
    decision_summary = maybe_text(decision.get("decision_summary"))
    if publication_posture == "release":
        return decision_summary or f"Round {maybe_text(decision.get('round_id'))} is ready for final publication with {len(report_rows)} role reports."
    open_risks = handoff.get("open_risks", []) if isinstance(handoff.get("open_risks"), list) else []
    reasons = "; ".join(maybe_text(item.get("summary")) for item in open_risks[:3] if isinstance(item, dict) and maybe_text(item.get("summary")))
    if reasons:
        return f"Release is withheld for this round because {reasons}."
    return decision_summary or f"Release is withheld for round {maybe_text(decision.get('round_id')) or 'current'} pending another investigation pass."


def published_sections(publication_posture: str, report_rows: list[dict[str, Any]]) -> list[str]:
    sections = ["publication-summary", "council-decision", "audit-trace"]
    if report_rows:
        sections.insert(2, "role-reports")
    if publication_posture == "release":
        sections.insert(2 if report_rows else 1, "evidence-basis")
    else:
        sections.insert(1, "release-hold")
        sections.insert(2 if report_rows else 2, "open-risks")
    return unique_texts(sections)


def operator_review_hints(supervisor_state: dict[str, Any], handoff: dict[str, Any], publication_posture: str) -> list[str]:
    results: list[str] = []
    notes = supervisor_state.get("operator_notes", []) if isinstance(supervisor_state.get("operator_notes"), list) else []
    results.extend(maybe_text(item) for item in notes if maybe_text(item))
    if publication_posture != "release":
        risks = handoff.get("open_risks", []) if isinstance(handoff.get("open_risks"), list) else []
        results.extend(maybe_text(item.get("summary")) for item in risks[:3] if isinstance(item, dict) and maybe_text(item.get("summary")))
    return unique_texts(results)[:5]


def materialize_final_publication_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    reporting_handoff_path: str,
    decision_path: str,
    sociologist_report_path: str,
    environmentalist_report_path: str,
    promotion_path: str,
    supervisor_state_path: str,
    output_path: str,
    allow_overwrite: bool,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    handoff_file = resolve_path(run_dir_path, reporting_handoff_path, f"reporting/reporting_handoff_{round_id}.json")
    decision_file = resolve_path(run_dir_path, decision_path, f"reporting/council_decision_{round_id}.json")
    promotion_file = resolve_path(run_dir_path, promotion_path, f"promotion/promoted_evidence_basis_{round_id}.json")
    supervisor_file = resolve_path(run_dir_path, supervisor_state_path, f"runtime/supervisor_state_{round_id}.json")
    report_files = {
        "sociologist": resolve_path(run_dir_path, sociologist_report_path, f"reporting/expert_report_sociologist_{round_id}.json"),
        "environmentalist": resolve_path(run_dir_path, environmentalist_report_path, f"reporting/expert_report_environmentalist_{round_id}.json"),
    }
    output_file = resolve_path(run_dir_path, output_path, f"reporting/final_publication_{round_id}.json")

    warnings: list[dict[str, Any]] = []
    handoff_payload = load_json_if_exists(handoff_file)
    if not isinstance(handoff_payload, dict):
        warnings.append({"code": "missing-reporting-handoff", "message": f"No reporting handoff artifact was found at {handoff_file}."})
        return {
            "status": "blocked",
            "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "operation": "blocked", "output_path": str(output_file)},
            "receipt_id": "publication-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, "missing-handoff")[:20],
            "batch_id": "publicationbatch-" + stable_hash(SKILL_NAME, run_id, round_id, "missing-handoff")[:16],
            "artifact_refs": [],
            "canonical_ids": [],
            "warnings": warnings,
            "board_handoff": {"candidate_ids": [], "evidence_refs": [], "gap_hints": [warnings[0]["message"]], "challenge_hints": [], "suggested_next_skills": ["eco-materialize-reporting-handoff"]},
        }
    handoff = handoff_payload

    decision_payload = load_json_if_exists(decision_file)
    if not isinstance(decision_payload, dict):
        warnings.append({"code": "missing-canonical-decision", "message": f"No canonical council decision was found at {decision_file}."})
        return {
            "status": "blocked",
            "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "operation": "blocked", "output_path": str(output_file)},
            "receipt_id": "publication-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, "missing-decision")[:20],
            "batch_id": "publicationbatch-" + stable_hash(SKILL_NAME, run_id, round_id, "missing-decision")[:16],
            "artifact_refs": [],
            "canonical_ids": [],
            "warnings": warnings,
            "board_handoff": {"candidate_ids": [], "evidence_refs": [], "gap_hints": [warnings[0]["message"]], "challenge_hints": [], "suggested_next_skills": ["eco-publish-council-decision"]},
        }
    decision = decision_payload

    promotion_context = load_promotion_basis_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        promotion_path=promotion_path,
    )
    promotion_payload = (
        promotion_context.get("payload")
        if isinstance(promotion_context.get("payload"), dict)
        else None
    )
    if not isinstance(promotion_payload, dict):
        warnings.append({"code": "missing-promotion-basis", "message": f"No promotion basis artifact or DB record was found at {promotion_file}."})
        return {
            "status": "blocked",
            "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "operation": "blocked", "output_path": str(output_file)},
            "receipt_id": "publication-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, "missing-promotion")[:20],
            "batch_id": "publicationbatch-" + stable_hash(SKILL_NAME, run_id, round_id, "missing-promotion")[:16],
            "artifact_refs": [],
            "canonical_ids": [],
            "warnings": warnings,
            "board_handoff": {"candidate_ids": [], "evidence_refs": [], "gap_hints": [warnings[0]["message"]], "challenge_hints": [], "suggested_next_skills": ["eco-promote-evidence-basis"]},
        }
    promotion_basis = promotion_payload

    supervisor_state_payload = load_json_if_exists(supervisor_file)
    supervisor_state = supervisor_state_payload or {}
    if not supervisor_state_payload:
        warnings.append({"code": "missing-supervisor-state", "message": f"No supervisor state artifact was found at {supervisor_file}."})

    publication_readiness = maybe_text(decision.get("publication_readiness")) or "hold"
    publication_posture = "release" if publication_readiness == "ready" else "withhold"
    report_rows: list[dict[str, Any]] = []
    missing_ready_reports: list[str] = []
    report_payloads: dict[str, dict[str, Any]] = {}
    published_report_refs = decision.get("published_report_refs", []) if isinstance(decision.get("published_report_refs"), list) else []
    for role in ROLE_VALUES:
        report_payload = load_json_if_exists(report_files[role])
        if isinstance(report_payload, dict):
            report_payloads[role] = report_payload
            report_rows.append(report_summary(role, report_payload, report_files[role]))
        elif publication_posture == "release":
            missing_ready_reports.append(str(report_files[role]))
        else:
            warnings.append({"code": "missing-canonical-report", "message": f"Canonical expert report is missing at {report_files[role]} but release posture is still withheld."})
    if missing_ready_reports:
        warnings.extend({"code": "missing-canonical-report", "message": f"Required canonical expert report is missing at {path}."} for path in missing_ready_reports)
        return {
            "status": "blocked",
            "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "operation": "blocked", "output_path": str(output_file)},
            "receipt_id": "publication-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, "missing-reports")[:20],
            "batch_id": "publicationbatch-" + stable_hash(SKILL_NAME, run_id, round_id, "missing-reports")[:16],
            "artifact_refs": [],
            "canonical_ids": [maybe_text(decision.get("decision_id"))] if maybe_text(decision.get("decision_id")) else [],
            "warnings": warnings,
            "board_handoff": {"candidate_ids": [maybe_text(decision.get("decision_id"))] if maybe_text(decision.get("decision_id")) else [], "evidence_refs": [], "gap_hints": [item["message"] for item in warnings], "challenge_hints": [], "suggested_next_skills": ["eco-publish-expert-report"]},
        }

    contract_fields = reporting_contract_fields_from_payload(
        decision_payload,
        fallback_payload=handoff_payload,
        observed_inputs_overrides={
            "reporting_handoff_artifact_present": handoff_file.exists(),
            "reporting_handoff_present": isinstance(handoff_payload, dict),
            "decision_artifact_present": decision_file.exists(),
            "decision_present": isinstance(decision_payload, dict),
            "promotion_artifact_present": bool(
                promotion_context.get("artifact_present")
            ),
            "promotion_present": bool(promotion_context.get("payload_present")),
            "supervisor_state_artifact_present": supervisor_file.exists(),
            "supervisor_state_present": isinstance(supervisor_state_payload, dict),
            "sociologist_report_artifact_present": report_files["sociologist"].exists(),
            "sociologist_report_present": isinstance(
                report_payloads.get("sociologist"), dict
            ),
            "environmentalist_report_artifact_present": report_files[
                "environmentalist"
            ].exists(),
            "environmentalist_report_present": isinstance(
                report_payloads.get("environmentalist"), dict
            ),
        },
        field_overrides={
            "reporting_handoff_source": (
                "reporting-handoff-artifact"
                if handoff_file.exists()
                else "missing-reporting-handoff"
            ),
            "decision_source": (
                "council-decision-artifact"
                if decision_file.exists()
                else "missing-canonical-decision"
            ),
            "promotion_source": maybe_text(promotion_context.get("source"))
            or "missing-promotion",
            "supervisor_state_source": (
                "supervisor-state-artifact"
                if supervisor_file.exists()
                else "missing-supervisor-state"
            ),
            "sociologist_report_source": (
                "expert-report-artifact"
                if report_files["sociologist"].exists()
                else "missing-sociologist-report"
            ),
            "environmentalist_report_source": (
                "expert-report-artifact"
                if report_files["environmentalist"].exists()
                else "missing-environmentalist-report"
            ),
        },
    )
    publication_id = "final-publication-" + stable_hash(run_id, round_id, publication_posture, maybe_text(decision.get("decision_id")))[:12]
    selected_evidence_refs = unique_texts(
        handoff.get("selected_evidence_refs", []) if isinstance(handoff.get("selected_evidence_refs"), list) else []
    )
    if not selected_evidence_refs:
        selected_evidence_refs = unique_texts(
            decision.get("selected_evidence_refs", []) if isinstance(decision.get("selected_evidence_refs"), list) else promotion_basis.get("selected_evidence_refs", []) if isinstance(promotion_basis.get("selected_evidence_refs"), list) else []
        )

    publication_payload = {
        "schema_version": "e1.2",
        "skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "publication_id": publication_id,
        "publication_status": "ready-for-release" if publication_posture == "release" else "hold-release",
        "publication_posture": publication_posture,
        **contract_fields,
        "publication_summary": release_summary(
            decision=decision,
            handoff=handoff,
            publication_posture=publication_posture,
            report_rows=report_rows,
        ),
        "published_sections": published_sections(publication_posture, report_rows),
        "decision": {
            "decision_id": maybe_text(decision.get("decision_id")),
            "moderator_status": maybe_text(decision.get("moderator_status")),
            "publication_readiness": publication_readiness,
            "decision_summary": maybe_text(decision.get("decision_summary")),
        },
        "role_reports": report_rows,
        "published_report_refs": unique_texts(published_report_refs or [row["report_path"] for row in report_rows]),
        "key_findings": handoff.get("key_findings", []) if isinstance(handoff.get("key_findings"), list) else [],
        "open_risks": handoff.get("open_risks", []) if isinstance(handoff.get("open_risks"), list) else [],
        "recommended_next_actions": handoff.get("recommended_next_actions", []) if isinstance(handoff.get("recommended_next_actions"), list) else [],
        "selected_evidence_refs": selected_evidence_refs,
        "operator_review_hints": operator_review_hints(supervisor_state, handoff, publication_posture),
        "audit_refs": {
            "reporting_handoff_path": str(handoff_file),
            "decision_path": str(decision_file),
            "promotion_path": str(promotion_file),
            "supervisor_state_path": str(supervisor_file),
            "role_report_paths": {
                role: str(path)
                for role, path in report_files.items()
                if isinstance(report_payloads.get(role), dict)
            },
        },
    }

    existing = load_json_if_exists(output_file)
    operation = "published"
    overwrote_existing = False
    if isinstance(existing, dict):
        if payload_without_generated_at(existing) == payload_without_generated_at(publication_payload):
            operation = "noop"
            publication_payload["generated_at_utc"] = maybe_text(existing.get("generated_at_utc")) or publication_payload["generated_at_utc"]
        elif allow_overwrite:
            overwrote_existing = True
        else:
            warnings.append({"code": "overwrite-blocked", "message": "Refusing to overwrite non-matching final publication without --allow-overwrite."})
            return {
                "status": "blocked",
                "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "operation": "blocked", "output_path": str(output_file)},
                "receipt_id": "publication-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, "overwrite-blocked")[:20],
                "batch_id": "publicationbatch-" + stable_hash(SKILL_NAME, run_id, round_id, "overwrite-blocked")[:16],
                "artifact_refs": [{"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"}],
                "canonical_ids": [publication_id],
                "warnings": warnings,
                "board_handoff": {"candidate_ids": [publication_id], "evidence_refs": [], "gap_hints": [warnings[-1]["message"]], "challenge_hints": [], "suggested_next_skills": ["eco-materialize-final-publication"]},
            }

    if operation != "noop":
        write_json_file(output_file, publication_payload)

    artifact_refs = [{"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"}]
    suggested_next_skills = ["eco-post-board-note"] if publication_posture == "release" else ["eco-post-board-note", "eco-propose-next-actions", "eco-open-falsification-probe"]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "operation": operation,
            "overwrote_existing": overwrote_existing,
            "output_path": str(output_file),
            "publication_id": publication_id,
            "publication_status": publication_payload["publication_status"],
            "publication_posture": publication_posture,
            "board_state_source": contract_fields["board_state_source"],
            "coverage_source": contract_fields["coverage_source"],
            "reporting_handoff_source": maybe_text(
                contract_fields.get("reporting_handoff_source")
            ),
            "decision_source": maybe_text(contract_fields.get("decision_source")),
            "promotion_source": maybe_text(contract_fields.get("promotion_source")),
            "supervisor_state_source": maybe_text(
                contract_fields.get("supervisor_state_source")
            ),
            "sociologist_report_source": maybe_text(
                contract_fields.get("sociologist_report_source")
            ),
            "environmentalist_report_source": maybe_text(
                contract_fields.get("environmentalist_report_source")
            ),
            "db_path": contract_fields["db_path"],
        },
        "receipt_id": "publication-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, operation, publication_id)[:20],
        "batch_id": "publicationbatch-" + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [publication_id],
        "warnings": warnings,
        "deliberation_sync": contract_fields["deliberation_sync"],
        "analysis_sync": contract_fields["analysis_sync"],
        "board_handoff": {
            "candidate_ids": [publication_id],
            "evidence_refs": artifact_refs,
            "gap_hints": [maybe_text(item.get("summary")) for item in publication_payload.get("open_risks", [])[:3] if isinstance(item, dict) and maybe_text(item.get("summary"))] if publication_posture != "release" else [],
            "challenge_hints": publication_payload.get("operator_review_hints", []),
            "suggested_next_skills": suggested_next_skills,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize a final publication artifact from canonical reporting outputs.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--reporting-handoff-path", default="")
    parser.add_argument("--decision-path", default="")
    parser.add_argument("--sociologist-report-path", default="")
    parser.add_argument("--environmentalist-report-path", default="")
    parser.add_argument("--promotion-path", default="")
    parser.add_argument("--supervisor-state-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--allow-overwrite", action="store_true")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = materialize_final_publication_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        reporting_handoff_path=args.reporting_handoff_path,
        decision_path=args.decision_path,
        sociologist_report_path=args.sociologist_report_path,
        environmentalist_report_path=args.environmentalist_report_path,
        promotion_path=args.promotion_path,
        supervisor_state_path=args.supervisor_state_path,
        output_path=args.output_path,
        allow_overwrite=args.allow_overwrite,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
