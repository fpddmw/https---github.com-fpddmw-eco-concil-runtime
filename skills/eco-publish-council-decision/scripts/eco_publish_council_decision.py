#!/usr/bin/env python3
"""Publish a canonical council decision from the decision draft."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "eco-publish-council-decision"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.reporting_contracts import (  # noqa: E402
    reporting_contract_fields_from_payload,
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


def publish_council_decision_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    draft_path: str,
    sociologist_report_path: str,
    environmentalist_report_path: str,
    output_path: str,
    allow_overwrite: bool,
    skip_report_check: bool,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    draft_file = resolve_path(run_dir_path, draft_path, f"reporting/council_decision_draft_{round_id}.json")
    sociologist_file = resolve_path(run_dir_path, sociologist_report_path, f"reporting/expert_report_sociologist_{round_id}.json")
    environmentalist_file = resolve_path(run_dir_path, environmentalist_report_path, f"reporting/expert_report_environmentalist_{round_id}.json")
    output_file = resolve_path(run_dir_path, output_path, f"reporting/council_decision_{round_id}.json")

    warnings: list[dict[str, Any]] = []
    draft_payload = load_json_if_exists(draft_file)
    if not isinstance(draft_payload, dict):
        warnings.append({"code": "missing-decision-draft", "message": f"No council decision draft was found at {draft_file}."})
        return {
            "status": "blocked",
            "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "operation": "blocked", "output_path": str(output_file)},
            "receipt_id": "reporting-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, "blocked")[:20],
            "batch_id": "reportingbatch-" + stable_hash(SKILL_NAME, run_id, round_id, "blocked")[:16],
            "artifact_refs": [],
            "canonical_ids": [],
            "warnings": warnings,
            "board_handoff": {"candidate_ids": [], "evidence_refs": [], "gap_hints": [warnings[0]["message"]], "challenge_hints": [], "suggested_next_skills": ["eco-draft-council-decision"]},
        }

    if maybe_text(draft_payload.get("round_id")) != round_id:
        warnings.append({"code": "round-mismatch", "message": "Council decision draft round_id does not match the requested round."})
    if warnings:
        return {
            "status": "blocked",
            "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "operation": "blocked", "output_path": str(output_file)},
            "receipt_id": "reporting-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, "mismatch")[:20],
            "batch_id": "reportingbatch-" + stable_hash(SKILL_NAME, run_id, round_id, "mismatch")[:16],
            "artifact_refs": [],
            "canonical_ids": [],
            "warnings": warnings,
            "board_handoff": {"candidate_ids": [], "evidence_refs": [], "gap_hints": [item["message"] for item in warnings], "challenge_hints": [], "suggested_next_skills": ["eco-draft-council-decision"]},
        }

    publication_readiness = maybe_text(draft_payload.get("publication_readiness")) or "hold"
    report_refs: list[str] = []
    report_payloads: dict[str, dict[str, Any]] = {}
    sociologist_payload = load_json_if_exists(sociologist_file)
    if isinstance(sociologist_payload, dict):
        report_payloads["sociologist"] = sociologist_payload
    environmentalist_payload = load_json_if_exists(environmentalist_file)
    if isinstance(environmentalist_payload, dict):
        report_payloads["environmentalist"] = environmentalist_payload
    if publication_readiness == "ready" and not skip_report_check:
        for path, payload in (
            (sociologist_file, sociologist_payload),
            (environmentalist_file, environmentalist_payload),
        ):
            if not isinstance(payload, dict):
                warnings.append({"code": "missing-canonical-report", "message": f"Required canonical expert report is missing at {path}."})
            else:
                report_refs.append(str(path))
        if warnings:
            return {
                "status": "blocked",
                "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "operation": "blocked", "output_path": str(output_file)},
                "receipt_id": "reporting-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, "missing-reports")[:20],
                "batch_id": "reportingbatch-" + stable_hash(SKILL_NAME, run_id, round_id, "missing-reports")[:16],
                "artifact_refs": [],
                "canonical_ids": [maybe_text(draft_payload.get("decision_id"))] if maybe_text(draft_payload.get("decision_id")) else [],
                "warnings": warnings,
                "board_handoff": {"candidate_ids": [maybe_text(draft_payload.get("decision_id"))] if maybe_text(draft_payload.get("decision_id")) else [], "evidence_refs": [], "gap_hints": [item["message"] for item in warnings], "challenge_hints": [], "suggested_next_skills": ["eco-publish-expert-report"]},
            }

    contract_fields = reporting_contract_fields_from_payload(
        draft_payload,
        observed_inputs_overrides={
            "decision_artifact_present": draft_file.exists(),
            "decision_present": isinstance(draft_payload, dict),
            "sociologist_report_artifact_present": sociologist_file.exists(),
            "sociologist_report_present": isinstance(sociologist_payload, dict),
            "environmentalist_report_artifact_present": environmentalist_file.exists(),
            "environmentalist_report_present": isinstance(
                environmentalist_payload, dict
            ),
        },
        field_overrides={
            "decision_source": (
                "council-decision-draft-artifact"
                if draft_file.exists()
                else "missing-decision-draft"
            ),
            "sociologist_report_source": (
                "expert-report-artifact"
                if sociologist_file.exists()
                else "missing-sociologist-report"
            ),
            "environmentalist_report_source": (
                "expert-report-artifact"
                if environmentalist_file.exists()
                else "missing-environmentalist-report"
            ),
        },
    )
    canonical_payload = {
        **draft_payload,
        **contract_fields,
        "canonical_artifact": "council-decision",
        "published_report_refs": unique_texts(report_refs),
    }
    existing = load_json_if_exists(output_file)
    operation = "published"
    overwrote_existing = False
    if isinstance(existing, dict):
        if existing == canonical_payload:
            operation = "noop"
        elif allow_overwrite:
            overwrote_existing = True
        else:
            warnings.append({"code": "overwrite-blocked", "message": "Refusing to overwrite non-matching canonical council decision without --allow-overwrite."})
            return {
                "status": "blocked",
                "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "operation": "blocked", "output_path": str(output_file)},
                "receipt_id": "reporting-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, "overwrite-blocked")[:20],
                "batch_id": "reportingbatch-" + stable_hash(SKILL_NAME, run_id, round_id, "overwrite-blocked")[:16],
                "artifact_refs": [{"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"}],
                "canonical_ids": [maybe_text(draft_payload.get("decision_id"))] if maybe_text(draft_payload.get("decision_id")) else [],
                "warnings": warnings,
                "board_handoff": {"candidate_ids": [maybe_text(draft_payload.get("decision_id"))] if maybe_text(draft_payload.get("decision_id")) else [], "evidence_refs": [], "gap_hints": [warnings[0]["message"]], "challenge_hints": [], "suggested_next_skills": ["eco-publish-council-decision"]},
            }
    if operation != "noop":
        write_json_file(output_file, canonical_payload)

    decision_id = maybe_text(draft_payload.get("decision_id"))
    artifact_refs = [{"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"}]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "operation": operation,
            "overwrote_existing": overwrote_existing,
            "output_path": str(output_file),
            "decision_id": decision_id,
            "publication_readiness": publication_readiness,
            "board_state_source": contract_fields["board_state_source"],
            "coverage_source": contract_fields["coverage_source"],
            "reporting_handoff_source": maybe_text(
                contract_fields.get("reporting_handoff_source")
            ),
            "promotion_source": maybe_text(contract_fields.get("promotion_source")),
            "decision_source": maybe_text(contract_fields.get("decision_source")),
            "sociologist_report_source": maybe_text(
                contract_fields.get("sociologist_report_source")
            ),
            "environmentalist_report_source": maybe_text(
                contract_fields.get("environmentalist_report_source")
            ),
            "db_path": contract_fields["db_path"],
        },
        "receipt_id": "reporting-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, operation, decision_id)[:20],
        "batch_id": "reportingbatch-" + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [decision_id] if decision_id else [],
        "warnings": warnings,
        "deliberation_sync": contract_fields["deliberation_sync"],
        "analysis_sync": contract_fields["analysis_sync"],
        "board_handoff": {
            "candidate_ids": [decision_id] if decision_id else [],
            "evidence_refs": artifact_refs,
            "gap_hints": [],
            "challenge_hints": [],
            "suggested_next_skills": ["eco-materialize-final-publication"] if publication_readiness == "ready" else ["eco-materialize-final-publication", "eco-post-board-note", "eco-propose-next-actions", "eco-open-falsification-probe"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish a canonical council decision from the current decision draft.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--draft-path", default="")
    parser.add_argument("--sociologist-report-path", default="")
    parser.add_argument("--environmentalist-report-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--allow-overwrite", action="store_true")
    parser.add_argument("--skip-report-check", action="store_true")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = publish_council_decision_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        draft_path=args.draft_path,
        sociologist_report_path=args.sociologist_report_path,
        environmentalist_report_path=args.environmentalist_report_path,
        output_path=args.output_path,
        allow_overwrite=args.allow_overwrite,
        skip_report_check=args.skip_report_check,
    )
    print(pretty_json(payload, args.pretty))
    return 0 if maybe_text(payload.get("status")) != "blocked" else 0


if __name__ == "__main__":
    raise SystemExit(main())
