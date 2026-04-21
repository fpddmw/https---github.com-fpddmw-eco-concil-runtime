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

from eco_council_runtime.council_objects import store_decision_trace_records  # noqa: E402
from eco_council_runtime.kernel.reporting_contracts import (  # noqa: E402
    reporting_contract_fields_from_payload,
)
from eco_council_runtime.phase2_promotion_resolution import (  # noqa: E402
    load_council_proposals,
    load_council_readiness_opinions,
    resolve_promotion_council_inputs,
)
from eco_council_runtime.kernel.deliberation_plane import (  # noqa: E402
    normalized_council_decision_payload,
    store_council_decision_record,
)
from eco_council_runtime.kernel.phase2_state_surfaces import (  # noqa: E402
    load_council_decision_wrapper,
    load_expert_report_wrapper,
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


def list_items(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def maybe_number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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


def deterministic_trace_id(
    run_id: str,
    round_id: str,
    decision_id: str,
    trace_index: int,
) -> str:
    return "decision-trace-" + stable_hash(
        "decision-trace",
        run_id,
        round_id,
        decision_id,
        trace_index,
    )[:12]


def selected_trace_object(
    *,
    publication_readiness: str,
    promoted_basis_id: str,
    supporting_proposal_ids: list[str],
    rejected_proposal_ids: list[str],
    supporting_opinion_ids: list[str],
    rejected_opinion_ids: list[str],
) -> tuple[str, str]:
    if publication_readiness != "ready" and rejected_proposal_ids:
        return "proposal", rejected_proposal_ids[0]
    if publication_readiness != "ready" and rejected_opinion_ids:
        return "readiness-opinion", rejected_opinion_ids[0]
    if supporting_opinion_ids:
        return "readiness-opinion", supporting_opinion_ids[0]
    if supporting_proposal_ids:
        return "proposal", supporting_proposal_ids[0]
    if promoted_basis_id:
        return "promotion-basis", promoted_basis_id
    return "promotion-basis", ""


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
    promotion_file = resolve_path(
        run_dir_path,
        "",
        f"promotion/promoted_evidence_basis_{round_id}.json",
    )

    warnings: list[dict[str, Any]] = []
    draft_context = load_council_decision_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        decision_stage="draft",
        decision_path=draft_path,
    )
    draft_payload = (
        draft_context.get("payload")
        if isinstance(draft_context.get("payload"), dict)
        else None
    )
    if not isinstance(draft_payload, dict):
        missing_message = (
            "No council decision draft DB record was found for "
            f"{draft_file}; artifact exists but is orphaned from the reporting plane."
            if bool(draft_context.get("artifact_present"))
            else (
                "No council decision draft artifact or DB record was found "
                f"at {draft_file}."
            )
        )
        warnings.append(
            {
                "code": "missing-decision-draft",
                "message": missing_message,
            }
        )
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
    promotion_context = load_promotion_basis_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    promotion_payload = (
        promotion_context.get("payload")
        if isinstance(promotion_context.get("payload"), dict)
        else None
    )
    report_refs: list[str] = []
    report_payloads: dict[str, dict[str, Any]] = {}
    sociologist_context = load_expert_report_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        agent_role="sociologist",
        report_stage="canonical",
        report_path=sociologist_report_path,
    )
    sociologist_payload = (
        sociologist_context.get("payload")
        if isinstance(sociologist_context.get("payload"), dict)
        else None
    )
    if isinstance(sociologist_payload, dict):
        report_payloads["sociologist"] = sociologist_payload
    environmentalist_context = load_expert_report_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        agent_role="environmentalist",
        report_stage="canonical",
        report_path=environmentalist_report_path,
    )
    environmentalist_payload = (
        environmentalist_context.get("payload")
        if isinstance(environmentalist_context.get("payload"), dict)
        else None
    )
    if isinstance(environmentalist_payload, dict):
        report_payloads["environmentalist"] = environmentalist_payload
    if publication_readiness == "ready" and not skip_report_check:
        for context, payload in (
            (sociologist_context, sociologist_payload),
            (environmentalist_context, environmentalist_payload),
        ):
            if not isinstance(payload, dict):
                artifact_path = str(context.get("artifact_path", ""))
                if bool(context.get("artifact_present")):
                    message = (
                        "Required canonical expert report has no DB record at "
                        f"{artifact_path}; the artifact is orphaned from the reporting plane."
                    )
                else:
                    message = (
                        f"Required canonical expert report is missing at {artifact_path}."
                    )
                warnings.append(
                    {"code": "missing-canonical-report", "message": message}
                )
            else:
                report_refs.append(str(context.get("artifact_path", "")))
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
            "decision_artifact_present": bool(draft_context.get("artifact_present")),
            "decision_present": bool(draft_context.get("payload_present")),
            "sociologist_report_artifact_present": bool(
                sociologist_context.get("artifact_present")
            ),
            "sociologist_report_present": bool(
                sociologist_context.get("payload_present")
            ),
            "environmentalist_report_artifact_present": bool(
                environmentalist_context.get("artifact_present")
            ),
            "environmentalist_report_present": bool(
                environmentalist_context.get("payload_present")
            ),
        },
        field_overrides={
            "decision_source": maybe_text(draft_context.get("source"))
            or "missing-decision-draft",
            "sociologist_report_source": maybe_text(sociologist_context.get("source"))
            or "missing-sociologist-report",
            "environmentalist_report_source": maybe_text(
                environmentalist_context.get("source")
            )
            or "missing-environmentalist-report",
        },
    )
    draft_supporting_proposal_ids = unique_texts(
        list_items(draft_payload.get("supporting_proposal_ids"))
    )
    draft_rejected_proposal_ids = unique_texts(
        list_items(draft_payload.get("rejected_proposal_ids"))
    )
    draft_supporting_opinion_ids = unique_texts(
        list_items(draft_payload.get("supporting_opinion_ids"))
    )
    draft_rejected_opinion_ids = unique_texts(
        list_items(draft_payload.get("rejected_opinion_ids"))
    )
    promoted_basis_id = maybe_text(draft_payload.get("promoted_basis_id")) or (
        maybe_text(promotion_payload.get("basis_id"))
        if isinstance(promotion_payload, dict)
        else ""
    )
    selected_basis_object_ids = unique_texts(
        list_items(draft_payload.get("selected_basis_object_ids"))
        + list_items(
            promotion_payload.get("selected_basis_object_ids")
            if isinstance(promotion_payload, dict)
            else []
        )
    )
    all_proposals = load_council_proposals(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    all_opinions = load_council_readiness_opinions(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    fallback_promotion_resolution = resolve_promotion_council_inputs(
        all_proposals,
        all_opinions,
        readiness_status=(
            maybe_text(draft_payload.get("readiness_status"))
            or maybe_text(promotion_payload.get("readiness_status"))
            or ("ready" if publication_readiness == "ready" else "needs-more-data")
        ),
        allow_non_ready=bool(
            isinstance(promotion_payload, dict)
            and maybe_text(promotion_payload.get("promotion_status")) == "promoted"
            and maybe_text(promotion_payload.get("readiness_status")) != "ready"
        ),
        round_id=round_id,
        basis_id=promoted_basis_id,
        selected_basis_object_ids=selected_basis_object_ids,
    )
    supporting_proposal_ids = (
        draft_supporting_proposal_ids
        or unique_texts(
            promotion_payload.get("supporting_proposal_ids", [])
            if isinstance(promotion_payload, dict)
            and isinstance(promotion_payload.get("supporting_proposal_ids"), list)
            else []
        )
        or unique_texts(
            fallback_promotion_resolution.get("supporting_proposal_ids", [])
            if isinstance(
                fallback_promotion_resolution.get("supporting_proposal_ids"), list
            )
            else []
        )
    )
    rejected_proposal_ids = (
        draft_rejected_proposal_ids
        or unique_texts(
            promotion_payload.get("rejected_proposal_ids", [])
            if isinstance(promotion_payload, dict)
            and isinstance(promotion_payload.get("rejected_proposal_ids"), list)
            else []
        )
        or unique_texts(
            fallback_promotion_resolution.get("rejected_proposal_ids", [])
            if isinstance(
                fallback_promotion_resolution.get("rejected_proposal_ids"), list
            )
            else []
        )
    )
    supporting_opinion_ids = (
        draft_supporting_opinion_ids
        or unique_texts(
            promotion_payload.get("supporting_opinion_ids", [])
            if isinstance(promotion_payload, dict)
            and isinstance(promotion_payload.get("supporting_opinion_ids"), list)
            else []
        )
        or unique_texts(
            fallback_promotion_resolution.get("supporting_opinion_ids", [])
            if isinstance(
                fallback_promotion_resolution.get("supporting_opinion_ids"), list
            )
            else []
        )
    )
    rejected_opinion_ids = (
        draft_rejected_opinion_ids
        or unique_texts(
            promotion_payload.get("rejected_opinion_ids", [])
            if isinstance(promotion_payload, dict)
            and isinstance(promotion_payload.get("rejected_opinion_ids"), list)
            else []
        )
        or unique_texts(
            fallback_promotion_resolution.get("rejected_opinion_ids", [])
            if isinstance(
                fallback_promotion_resolution.get("rejected_opinion_ids"), list
            )
            else []
        )
    )
    selected_object_kind, selected_object_id = selected_trace_object(
        publication_readiness=publication_readiness,
        promoted_basis_id=promoted_basis_id,
        supporting_proposal_ids=supporting_proposal_ids,
        rejected_proposal_ids=rejected_proposal_ids,
        supporting_opinion_ids=supporting_opinion_ids,
        rejected_opinion_ids=rejected_opinion_ids,
    )
    accepted_object_ids = unique_texts(
        [promoted_basis_id, *selected_basis_object_ids]
        + supporting_proposal_ids
        + supporting_opinion_ids
    )
    rejected_object_ids = unique_texts(
        rejected_proposal_ids + rejected_opinion_ids
    )
    accepted_proposals = [
        proposal
        for proposal in all_proposals
        if maybe_text(proposal.get("proposal_id")) in supporting_proposal_ids
    ]
    rejected_proposals = [
        proposal
        for proposal in all_proposals
        if maybe_text(proposal.get("proposal_id")) in rejected_proposal_ids
    ]
    accepted_opinions = [
        opinion
        for opinion in all_opinions
        if maybe_text(opinion.get("opinion_id")) in supporting_opinion_ids
    ]
    rejected_opinions = [
        opinion
        for opinion in all_opinions
        if maybe_text(opinion.get("opinion_id")) in rejected_opinion_ids
    ]
    trace_confidence = None
    for row in [*rejected_proposals, *accepted_opinions, *accepted_proposals, *rejected_opinions]:
        candidate = maybe_number(row.get("confidence"))
        if candidate is not None:
            trace_confidence = candidate
            break
    decision_id = maybe_text(draft_payload.get("decision_id"))
    trace_id = deterministic_trace_id(run_id, round_id, decision_id, 0)
    canonical_payload = normalized_council_decision_payload(
        {
            **draft_payload,
            **contract_fields,
            "record_id": "",
            "provenance": {},
            "decision_stage": "canonical",
            "canonical_artifact": "council-decision",
            "published_report_refs": unique_texts(report_refs),
            "promoted_basis_id": promoted_basis_id,
            "selected_basis_object_ids": selected_basis_object_ids,
            "supporting_proposal_ids": supporting_proposal_ids,
            "rejected_proposal_ids": rejected_proposal_ids,
            "supporting_opinion_ids": supporting_opinion_ids,
            "rejected_opinion_ids": rejected_opinion_ids,
            "promotion_resolution_mode": maybe_text(
                draft_payload.get("promotion_resolution_mode")
            )
            or maybe_text(
                promotion_payload.get("promotion_resolution_mode")
                if isinstance(promotion_payload, dict)
                else ""
            )
            or maybe_text(
                fallback_promotion_resolution.get("promotion_resolution_mode")
            ),
            "promotion_resolution_reasons": (
                draft_payload.get("promotion_resolution_reasons", [])
                if isinstance(draft_payload.get("promotion_resolution_reasons"), list)
                else promotion_payload.get("promotion_resolution_reasons", [])
                if isinstance(promotion_payload, dict)
                and isinstance(
                    promotion_payload.get("promotion_resolution_reasons"), list
                )
                else fallback_promotion_resolution.get(
                    "promotion_resolution_reasons", []
                )
                if isinstance(
                    fallback_promotion_resolution.get(
                        "promotion_resolution_reasons"
                    ),
                    list,
                )
                else []
            ),
            "council_input_counts": (
                draft_payload.get("council_input_counts", {})
                if isinstance(draft_payload.get("council_input_counts"), dict)
                else promotion_payload.get("council_input_counts", {})
                if isinstance(promotion_payload, dict)
                and isinstance(promotion_payload.get("council_input_counts"), dict)
                else fallback_promotion_resolution.get("council_input_counts", {})
                if isinstance(
                    fallback_promotion_resolution.get("council_input_counts"), dict
                )
                else {}
            ),
            "accepted_object_ids": accepted_object_ids,
            "rejected_object_ids": rejected_object_ids,
            "decision_trace_ids": [trace_id] if decision_id else [],
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
    decision_trace_bundle = {
        "run_id": run_id,
        "round_id": round_id,
        "traces": [
            {
                "trace_id": trace_id,
                "decision_id": decision_id,
                "decision_kind": "council-decision",
                "status": "published" if publication_readiness == "ready" else "withheld",
                "selected_object_kind": selected_object_kind,
                "selected_object_id": selected_object_id,
                "confidence": trace_confidence,
                "rationale": maybe_text(canonical_payload.get("decision_summary"))
                or maybe_text(draft_payload.get("decision_summary"))
                or "Council decision was published from the current draft state.",
                "decision_source": "council-trace",
                "accepted_object_ids": accepted_object_ids,
                "rejected_object_ids": rejected_object_ids,
                "evidence_refs": unique_texts(
                    list_items(canonical_payload.get("selected_evidence_refs"))
                    + [
                        ref
                        for row in [
                            *accepted_proposals,
                            *rejected_proposals,
                            *accepted_opinions,
                            *rejected_opinions,
                        ]
                        for ref in list_items(row.get("evidence_refs"))
                    ]
                ),
                "lineage": unique_texts(
                    [decision_id, promoted_basis_id, *selected_basis_object_ids]
                    + accepted_object_ids
                    + rejected_object_ids
                ),
                "provenance": {
                    "source_skill": SKILL_NAME,
                    "decision_source": maybe_text(canonical_payload.get("decision_source"))
                    or "deliberation-plane-council-decision-draft",
                    "publication_readiness": publication_readiness,
                    "promotion_source": maybe_text(contract_fields.get("promotion_source"))
                    or "missing-promotion",
                    "promoted_basis_id": promoted_basis_id,
                    "promotion_basis_path": str(promotion_file),
                    "supporting_proposal_count": len(supporting_proposal_ids),
                    "rejected_proposal_count": len(rejected_proposal_ids),
                    "supporting_opinion_count": len(supporting_opinion_ids),
                    "rejected_opinion_count": len(rejected_opinion_ids),
                },
            }
        ],
    }
    stored_payload = store_council_decision_record(
        run_dir_path,
        decision_payload=canonical_payload,
        artifact_path=str(output_file),
    )
    store_decision_trace_records(
        run_dir_path,
        trace_bundle=decision_trace_bundle,
        artifact_path=str(output_file),
    )
    if operation != "noop":
        write_json_file(output_file, stored_payload)

    artifact_refs = [{"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"}]
    decision_id = maybe_text(stored_payload.get("decision_id")) or decision_id
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
            "promotion_basis_path": str(promotion_file),
            "decision_source": maybe_text(contract_fields.get("decision_source")),
            "decision_trace_id": trace_id,
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
            "suggested_next_skills": ["eco-materialize-final-publication"] if publication_readiness == "ready" else ["eco-materialize-final-publication", "eco-submit-council-proposal", "eco-submit-readiness-opinion", "eco-propose-next-actions", "eco-open-falsification-probe"],
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
