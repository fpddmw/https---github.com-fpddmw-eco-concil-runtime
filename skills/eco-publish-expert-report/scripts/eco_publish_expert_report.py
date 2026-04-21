#!/usr/bin/env python3
"""Publish a canonical expert report from a role-specific draft."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "eco-publish-expert-report"
ROLE_VALUES = ("sociologist", "environmentalist")
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.reporting_contracts import (  # noqa: E402
    reporting_contract_fields_from_payload,
)
from eco_council_runtime.kernel.deliberation_plane import (  # noqa: E402
    normalized_expert_report_payload,
    store_expert_report_record,
)
from eco_council_runtime.kernel.phase2_state_surfaces import (  # noqa: E402
    load_expert_report_wrapper,
)


def normalize_space(value: Any) -> str:
    return " ".join(str(value).split())


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return normalize_space(value)


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


def publish_expert_report_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    role: str,
    draft_path: str,
    output_path: str,
    allow_overwrite: bool,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    draft_file = resolve_path(run_dir_path, draft_path, f"reporting/expert_report_draft_{role}_{round_id}.json")
    output_file = resolve_path(run_dir_path, output_path, f"reporting/expert_report_{role}_{round_id}.json")

    warnings: list[dict[str, Any]] = []
    draft_context = load_expert_report_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        agent_role=role,
        report_stage="draft",
        report_path=draft_path,
    )
    draft_payload = (
        draft_context.get("payload")
        if isinstance(draft_context.get("payload"), dict)
        else None
    )
    if not isinstance(draft_payload, dict):
        missing_message = (
            "No expert report draft DB record was found for "
            f"{draft_file}; artifact exists but is orphaned from the reporting plane."
            if bool(draft_context.get("artifact_present"))
            else (
                "No expert report draft artifact or DB record was found "
                f"at {draft_file}."
            )
        )
        warnings.append(
            {
                "code": "missing-report-draft",
                "message": missing_message,
            }
        )
        return {
            "status": "blocked",
            "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "role": role, "operation": "blocked", "output_path": str(output_file)},
            "receipt_id": "reporting-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, role, "blocked")[:20],
            "batch_id": "reportingbatch-" + stable_hash(SKILL_NAME, run_id, round_id, role, "blocked")[:16],
            "artifact_refs": [],
            "canonical_ids": [],
            "warnings": warnings,
            "board_handoff": {"candidate_ids": [], "evidence_refs": [], "gap_hints": [warnings[0]["message"]], "challenge_hints": [], "suggested_next_skills": ["eco-draft-expert-report"]},
        }

    if maybe_text(draft_payload.get("round_id")) != round_id:
        warnings.append({"code": "round-mismatch", "message": "Expert report draft round_id does not match the requested round."})
    if maybe_text(draft_payload.get("agent_role")) != role:
        warnings.append({"code": "role-mismatch", "message": "Expert report draft agent_role does not match the requested role."})
    if warnings:
        return {
            "status": "blocked",
            "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "role": role, "operation": "blocked", "output_path": str(output_file)},
            "receipt_id": "reporting-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, role, "mismatch")[:20],
            "batch_id": "reportingbatch-" + stable_hash(SKILL_NAME, run_id, round_id, role, "mismatch")[:16],
            "artifact_refs": [],
            "canonical_ids": [],
            "warnings": warnings,
            "board_handoff": {"candidate_ids": [], "evidence_refs": [], "gap_hints": [item["message"] for item in warnings], "challenge_hints": [], "suggested_next_skills": ["eco-draft-expert-report"]},
        }

    contract_fields = reporting_contract_fields_from_payload(
        draft_payload,
        observed_inputs_overrides={
            "expert_report_draft_artifact_present": bool(
                draft_context.get("artifact_present")
            ),
            "expert_report_draft_present": bool(draft_context.get("payload_present")),
        },
        field_overrides={
            "expert_report_draft_source": maybe_text(draft_context.get("source"))
            or "missing-expert-report-draft",
        },
    )
    canonical_payload = normalized_expert_report_payload(
        {
            **draft_payload,
            **contract_fields,
            "record_id": "",
            "provenance": {},
            "report_stage": "canonical",
            "canonical_artifact": "expert-report",
            "canonical_role": role,
        },
        run_id=run_id,
        round_id=round_id,
    )
    existing = load_json_if_exists(output_file)
    operation = "published"
    overwrote_existing = False
    if isinstance(existing, dict):
        if existing == canonical_payload:
            operation = "noop"
        elif allow_overwrite:
            overwrote_existing = True
        else:
            warnings.append({"code": "overwrite-blocked", "message": "Refusing to overwrite non-matching canonical expert report without --allow-overwrite."})
            return {
                "status": "blocked",
                "summary": {"skill": SKILL_NAME, "run_id": run_id, "round_id": round_id, "role": role, "operation": "blocked", "output_path": str(output_file)},
                "receipt_id": "reporting-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, role, "overwrite-blocked")[:20],
                "batch_id": "reportingbatch-" + stable_hash(SKILL_NAME, run_id, round_id, role, "overwrite-blocked")[:16],
                "artifact_refs": [{"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"}],
                "canonical_ids": [maybe_text(draft_payload.get("report_id"))] if maybe_text(draft_payload.get("report_id")) else [],
                "warnings": warnings,
                "board_handoff": {"candidate_ids": [maybe_text(draft_payload.get("report_id"))] if maybe_text(draft_payload.get("report_id")) else [], "evidence_refs": [], "gap_hints": [warnings[0]["message"]], "challenge_hints": [], "suggested_next_skills": ["eco-publish-expert-report"]},
            }
    stored_payload = store_expert_report_record(
        run_dir_path,
        report_payload=canonical_payload,
        artifact_path=str(output_file),
    )
    if operation != "noop":
        write_json_file(output_file, stored_payload)

    artifact_refs = [{"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"}]
    report_id = maybe_text(stored_payload.get("report_id")) or maybe_text(
        draft_payload.get("report_id")
    )
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "role": role,
            "operation": operation,
            "overwrote_existing": overwrote_existing,
            "output_path": str(output_file),
            "report_id": report_id,
            "board_state_source": contract_fields["board_state_source"],
            "coverage_source": contract_fields["coverage_source"],
            "reporting_handoff_source": maybe_text(
                contract_fields.get("reporting_handoff_source")
            ),
            "decision_source": maybe_text(contract_fields.get("decision_source")),
            "expert_report_draft_source": maybe_text(
                contract_fields.get("expert_report_draft_source")
            ),
            "db_path": contract_fields["db_path"],
        },
        "receipt_id": "reporting-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, role, operation, report_id)[:20],
        "batch_id": "reportingbatch-" + stable_hash(SKILL_NAME, run_id, round_id, role, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [report_id] if report_id else [],
        "warnings": warnings,
        "deliberation_sync": contract_fields["deliberation_sync"],
        "analysis_sync": contract_fields["analysis_sync"],
        "board_handoff": {
            "candidate_ids": [report_id] if report_id else [],
            "evidence_refs": artifact_refs,
            "gap_hints": [],
            "challenge_hints": [],
            "suggested_next_skills": ["eco-publish-council-decision"] if maybe_text(draft_payload.get("status")) == "ready-to-publish" else ["eco-submit-council-proposal", "eco-submit-readiness-opinion", "eco-propose-next-actions"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish a canonical expert report from a role-specific draft.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--role", required=True, choices=ROLE_VALUES)
    parser.add_argument("--draft-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--allow-overwrite", action="store_true")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = publish_expert_report_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        role=args.role,
        draft_path=args.draft_path,
        output_path=args.output_path,
        allow_overwrite=args.allow_overwrite,
    )
    print(pretty_json(payload, args.pretty))
    return 0 if maybe_text(payload.get("status")) != "blocked" else 0


if __name__ == "__main__":
    raise SystemExit(main())
