from __future__ import annotations

import json
from pathlib import Path
from typing import Any


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


def maybe_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    text = maybe_text(value).lower()
    if not text:
        return None
    if text in {"1", "true", "yes", "allow", "ready"}:
        return True
    if text in {"0", "false", "no", "block", "hold"}:
        return False
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


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def parse_json_dict(value: str, *, option_name: str) -> dict[str, Any]:
    text = maybe_text(value)
    if not text:
        return {}
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError(f"{option_name} must decode to a JSON object.")
    return dict(payload)


def parse_json_list(value: str, *, option_name: str) -> list[Any]:
    text = maybe_text(value)
    if not text:
        return []
    payload = json.loads(text)
    if not isinstance(payload, list):
        raise ValueError(f"{option_name} must decode to a JSON array.")
    return list(payload)


def merged_text_list(*values: Any) -> list[str]:
    flattened: list[Any] = []
    for value in values:
        if isinstance(value, list):
            flattened.extend(value)
        else:
            flattened.append(value)
    return unique_texts(flattened)


def merged_provenance(
    *,
    provenance_json: str,
    payload_provenance: Any,
    source_skill: str,
) -> dict[str, Any]:
    merged = (
        dict(payload_provenance)
        if isinstance(payload_provenance, dict)
        else {}
    )
    merged.update(parse_json_dict(provenance_json, option_name="--provenance-json"))
    merged.setdefault("source_skill", maybe_text(source_skill))
    return merged


def merged_target(
    *,
    target_json: str,
    target_kind: str,
    target_id: str,
    target_claim_id: str,
    target_hypothesis_id: str,
    target_ticket_id: str,
    target_task_id: str,
) -> dict[str, Any]:
    target = parse_json_dict(target_json, option_name="--target-json")
    resolved_kind = maybe_text(target_kind) or maybe_text(target.get("object_kind")) or maybe_text(target.get("kind"))
    resolved_id = maybe_text(target_id) or maybe_text(target.get("object_id")) or maybe_text(target.get("id"))
    if resolved_kind:
        target["object_kind"] = resolved_kind
    if resolved_id:
        target["object_id"] = resolved_id
    if maybe_text(target_claim_id):
        target["claim_id"] = maybe_text(target_claim_id)
    if maybe_text(target_hypothesis_id):
        target["hypothesis_id"] = maybe_text(target_hypothesis_id)
    if maybe_text(target_ticket_id):
        target["ticket_id"] = maybe_text(target_ticket_id)
    if maybe_text(target_task_id):
        target["task_id"] = maybe_text(target_task_id)
    return target


def council_proposal_payload(
    *,
    run_id: str,
    round_id: str,
    proposal_id: str,
    proposal_kind: str,
    agent_role: str,
    rationale: str,
    decision_source: str,
    status: str,
    confidence: Any,
    target_kind: str,
    target_id: str,
    target_claim_id: str,
    target_hypothesis_id: str,
    target_ticket_id: str,
    target_task_id: str,
    target_json: str,
    action_kind: str,
    assigned_role: str,
    objective: str,
    summary: str,
    evidence_refs: list[str],
    evidence_refs_json: str,
    response_to_ids: list[str],
    response_to_ids_json: str,
    lineage: list[str],
    lineage_json: str,
    provenance_json: str,
    extra_json: str,
    source_skill: str,
    report_basis_disposition: str,
    report_basis_freeze_allowed: Any,
    publication_readiness: str,
    handoff_status: str,
    moderator_status: str,
) -> dict[str, Any]:
    payload = parse_json_dict(extra_json, option_name="--extra-json")
    payload["run_id"] = maybe_text(run_id)
    payload["round_id"] = maybe_text(round_id)
    if maybe_text(proposal_id):
        payload["proposal_id"] = maybe_text(proposal_id)
    payload["proposal_kind"] = maybe_text(proposal_kind)
    payload["agent_role"] = maybe_text(agent_role)
    payload["rationale"] = maybe_text(rationale)
    payload["decision_source"] = maybe_text(decision_source) or "agent-council"
    payload["status"] = maybe_text(status) or maybe_text(payload.get("status")) or "open"

    if maybe_text(action_kind):
        payload["action_kind"] = maybe_text(action_kind)
    if maybe_text(assigned_role):
        payload["assigned_role"] = maybe_text(assigned_role)
    if maybe_text(objective):
        payload["objective"] = maybe_text(objective)
    if maybe_text(summary):
        payload["summary"] = maybe_text(summary)

    confidence_value = maybe_number(confidence)
    if confidence_value is not None:
        payload["confidence"] = confidence_value

    payload["evidence_refs"] = merged_text_list(
        payload.get("evidence_refs", []),
        parse_json_list(evidence_refs_json, option_name="--evidence-refs-json"),
        evidence_refs,
    )
    payload["response_to_ids"] = merged_text_list(
        payload.get("response_to_ids", []),
        parse_json_list(response_to_ids_json, option_name="--response-to-ids-json"),
        response_to_ids,
    )
    payload["lineage"] = merged_text_list(
        payload.get("lineage", []),
        parse_json_list(lineage_json, option_name="--lineage-json"),
        lineage,
    )
    payload["provenance"] = merged_provenance(
        provenance_json=provenance_json,
        payload_provenance=payload.get("provenance"),
        source_skill=source_skill,
    )

    target = merged_target(
        target_json=target_json,
        target_kind=target_kind,
        target_id=target_id,
        target_claim_id=target_claim_id,
        target_hypothesis_id=target_hypothesis_id,
        target_ticket_id=target_ticket_id,
        target_task_id=target_task_id,
    )
    if target:
        payload["target"] = target
    resolved_target_kind = (
        maybe_text(target_kind)
        or maybe_text(payload.get("target_kind"))
        or maybe_text(target.get("object_kind"))
    )
    resolved_target_id = (
        maybe_text(target_id)
        or maybe_text(payload.get("target_id"))
        or maybe_text(target.get("object_id"))
    )
    if resolved_target_kind:
        payload["target_kind"] = resolved_target_kind
    if resolved_target_id:
        payload["target_id"] = resolved_target_id
    if maybe_text(target_claim_id):
        payload["target_claim_id"] = maybe_text(target_claim_id)
    if maybe_text(target_hypothesis_id):
        payload["target_hypothesis_id"] = maybe_text(target_hypothesis_id)
    if maybe_text(target_ticket_id):
        payload["target_ticket_id"] = maybe_text(target_ticket_id)
    if maybe_text(target_task_id):
        payload["target_task_id"] = maybe_text(target_task_id)

    if maybe_text(report_basis_disposition):
        payload["report_basis_disposition"] = maybe_text(report_basis_disposition)
    report_basis_freeze_allowed_value = maybe_bool(report_basis_freeze_allowed)
    if report_basis_freeze_allowed_value is not None:
        payload["report_basis_freeze_allowed"] = report_basis_freeze_allowed_value
    if maybe_text(publication_readiness):
        payload["publication_readiness"] = maybe_text(publication_readiness)
    if maybe_text(handoff_status):
        payload["handoff_status"] = maybe_text(handoff_status)
    if maybe_text(moderator_status):
        payload["moderator_status"] = maybe_text(moderator_status)
    return payload


def readiness_opinion_payload(
    *,
    run_id: str,
    round_id: str,
    opinion_id: str,
    agent_role: str,
    readiness_status: str,
    rationale: str,
    decision_source: str,
    opinion_status: str,
    sufficient_for_report_basis: Any,
    confidence: Any,
    basis_object_ids: list[str],
    basis_object_ids_json: str,
    evidence_refs: list[str],
    evidence_refs_json: str,
    lineage: list[str],
    lineage_json: str,
    provenance_json: str,
    extra_json: str,
    source_skill: str,
) -> dict[str, Any]:
    payload = parse_json_dict(extra_json, option_name="--extra-json")
    payload["run_id"] = maybe_text(run_id)
    payload["round_id"] = maybe_text(round_id)
    if maybe_text(opinion_id):
        payload["opinion_id"] = maybe_text(opinion_id)
    payload["agent_role"] = maybe_text(agent_role)
    payload["readiness_status"] = maybe_text(readiness_status)
    payload["rationale"] = maybe_text(rationale)
    payload["decision_source"] = maybe_text(decision_source) or "agent-council"
    payload["opinion_status"] = (
        maybe_text(opinion_status)
        or maybe_text(payload.get("opinion_status"))
        or "submitted"
    )

    sufficient_value = maybe_bool(sufficient_for_report_basis)
    if sufficient_value is not None:
        payload["sufficient_for_report_basis"] = sufficient_value

    confidence_value = maybe_number(confidence)
    if confidence_value is not None:
        payload["confidence"] = confidence_value

    payload["basis_object_ids"] = merged_text_list(
        payload.get("basis_object_ids", []),
        parse_json_list(
            basis_object_ids_json,
            option_name="--basis-object-ids-json",
        ),
        basis_object_ids,
    )
    payload["evidence_refs"] = merged_text_list(
        payload.get("evidence_refs", []),
        parse_json_list(evidence_refs_json, option_name="--evidence-refs-json"),
        evidence_refs,
    )
    payload["lineage"] = merged_text_list(
        payload.get("lineage", []),
        parse_json_list(lineage_json, option_name="--lineage-json"),
        lineage,
    )
    payload["provenance"] = merged_provenance(
        provenance_json=provenance_json,
        payload_provenance=payload.get("provenance"),
        source_skill=source_skill,
    )
    return payload


__all__ = [
    "council_proposal_payload",
    "maybe_bool",
    "maybe_number",
    "maybe_text",
    "parse_json_dict",
    "parse_json_list",
    "readiness_opinion_payload",
    "resolve_path",
    "resolve_run_dir",
    "unique_texts",
    "write_json_file",
]
