#!/usr/bin/env python3
"""Materialize a compact DB-backed reporting handoff from frozen evidence basis."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "materialize-reporting-handoff"
WORKSPACE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "eco-concil-runtime").exists() and (parent / "skills").exists())
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.kernel.reporting.reporting_contracts import (  # noqa: E402
    reporting_contract_fields_from_payload,
)
from eco_council_runtime.objects.council import query_council_objects  # noqa: E402
from eco_council_runtime.kernel.planes.deliberation_plane import (  # noqa: E402
    store_reporting_handoff_record,
)
from eco_council_runtime.kernel.operator.surfaces import (  # noqa: E402
    load_report_basis_freeze_wrapper,
    load_round_readiness_wrapper,
    load_supervisor_state_wrapper,
)
from eco_council_runtime.reporting_status import (  # noqa: E402
    reporting_blocker_summaries,
    reporting_gate_state,
)
from eco_council_runtime.report_basis_policy import (  # noqa: E402
    accepted_limitations_from_constraints,
    report_basis_input_policy,
    unresolved_challenges_from_constraints,
)
from eco_council_runtime.reporting_objects import query_reporting_objects  # noqa: E402


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


def dict_items(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


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


def load_text_if_exists(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def load_json_if_exists(path_text: Any) -> dict[str, Any]:
    path_value = maybe_text(path_text)
    if not path_value:
        return {}
    path = Path(path_value)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def load_claim_gap_action_cards(run_dir: Path, round_id: str) -> dict[str, Any]:
    artifact_path = run_dir / "analytics" / f"claim_gap_action_cards_{round_id}.json"
    if not artifact_path.exists():
        return {
            "present": False,
            "path": str(artifact_path.resolve()),
            "action_cards": [],
        }
    try:
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {
            "present": False,
            "path": str(artifact_path.resolve()),
            "action_cards": [],
            "warning": "claim-gap action card artifact is not valid JSON",
        }
    cards = (
        payload.get("action_cards", [])
        if isinstance(payload, dict) and isinstance(payload.get("action_cards"), list)
        else []
    )
    return {
        "present": bool(cards),
        "path": str(artifact_path.resolve()),
        "action_cards": [card for card in cards if isinstance(card, dict)],
        "payload": payload if isinstance(payload, dict) else {},
    }


def load_interaction_timeline(run_dir: Path, round_id: str) -> dict[str, Any]:
    artifact_path = (
        run_dir / "analytics" / f"fact_policy_public_interaction_timeline_{round_id}.json"
    )
    if not artifact_path.exists():
        return {
            "present": False,
            "path": str(artifact_path.resolve()),
            "interaction_nodes": [],
            "parallel_timeline_nodes": [],
            "lane_episode_cards": [],
            "payload": {},
        }
    try:
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {
            "present": False,
            "path": str(artifact_path.resolve()),
            "interaction_nodes": [],
            "parallel_timeline_nodes": [],
            "lane_episode_cards": [],
            "payload": {},
            "warning": "interaction timeline artifact is not valid JSON",
        }
    if not isinstance(payload, dict):
        payload = {}
    interaction_nodes = [
        item
        for item in list_items(payload.get("interaction_nodes"))
        if isinstance(item, dict)
    ]
    parallel_nodes = [
        item
        for item in list_items(payload.get("parallel_timeline_nodes"))
        if isinstance(item, dict)
    ]
    lane_episode_cards = [
        item
        for item in list_items(payload.get("lane_episode_cards"))
        if isinstance(item, dict)
    ]
    return {
        "present": bool(interaction_nodes or lane_episode_cards),
        "path": str(artifact_path.resolve()),
        "interaction_nodes": interaction_nodes,
        "parallel_timeline_nodes": parallel_nodes,
        "lane_episode_cards": lane_episode_cards,
        "payload": payload,
    }


def load_analysis_item_artifact(
    run_dir: Path,
    round_id: str,
    *,
    file_stem: str,
    item_key: str,
) -> dict[str, Any]:
    artifact_path = run_dir / "analytics" / f"{file_stem}_{round_id}.json"
    if not artifact_path.exists():
        return {
            "present": False,
            "path": str(artifact_path.resolve()),
            "items": [],
            "payload": {},
        }
    try:
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {
            "present": False,
            "path": str(artifact_path.resolve()),
            "items": [],
            "payload": {},
            "warning": f"{file_stem} artifact is not valid JSON",
        }
    if not isinstance(payload, dict):
        payload = {}
    items = [item for item in list_items(payload.get(item_key)) if isinstance(item, dict)]
    return {
        "present": bool(items),
        "path": str(artifact_path.resolve()),
        "items": items,
        "payload": payload,
    }


def _brief_refs(nodes: list[dict[str, Any]], episodes: list[dict[str, Any]] | None = None, *, limit: int = 20) -> list[Any]:
    refs: list[Any] = []
    for episode in episodes or []:
        ref = episode.get("episode_ref")
        if isinstance(ref, dict):
            refs.append(ref)
        refs.extend(list_items(episode.get("evidence_refs")))
        if len(refs) >= limit:
            break
    for node in nodes:
        refs.extend(list_items(node.get("episode_refs")))
        refs.extend(list_items(node.get("fact_or_policy_evidence_refs")))
        refs.extend(list_items(node.get("public_or_media_evidence_refs")))
        refs.extend(list_items(node.get("evidence_refs")))
        if len(refs) >= limit:
            break
    seen: set[str] = set()
    results: list[Any] = []
    for ref in refs:
        key = json.dumps(ref, ensure_ascii=True, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        results.append(ref)
        if len(results) >= limit:
            break
    return results


def build_interaction_section_briefs(
    timeline_context: dict[str, Any],
) -> list[dict[str, Any]]:
    nodes = [
        item
        for item in list_items(timeline_context.get("interaction_nodes"))
        if isinstance(item, dict)
    ]
    if not nodes:
        return []
    payload = (
        timeline_context.get("payload")
        if isinstance(timeline_context.get("payload"), dict)
        else {}
    )
    observed_input_summary = (
        payload.get("observed_input_summary")
        if isinstance(payload.get("observed_input_summary"), dict)
        else {}
    )
    lane_episode_cards = [
        item
        for item in list_items(timeline_context.get("lane_episode_cards"))
        if isinstance(item, dict)
    ]
    limitations = unique_texts(
        [
            maybe_text(
                (
                    node.get("claim_boundary", {})
                    if isinstance(node.get("claim_boundary"), dict)
                    else {}
                ).get("report_boundary")
            )
            for node in nodes
        ]
        + [
            "Section brief is advisory; report prose must cite council-carried or reporting-basis objects before treating the timeline as report basis.",
            "Do not write causality, policy impact, public response attribution, representative public opinion, or evidence absence from timeline co-visibility alone.",
        ]
    )
    denominators = {
        "interaction_node_count": len(nodes),
        "lane_episode_card_count": len(lane_episode_cards),
        "lane_episode_counts": observed_input_summary.get("lane_episode_counts", {}),
        "parallel_timeline_node_count": len(
            list_items(timeline_context.get("parallel_timeline_nodes"))
        ),
        "environment_signal_count": observed_input_summary.get("environment_signal_count", 0),
        "formal_signal_count": observed_input_summary.get("formal_signal_count", 0),
        "public_signal_count": observed_input_summary.get("public_signal_count", 0),
        "missing_timestamp_count": observed_input_summary.get("missing_timestamp_count", 0),
        "helper_artifacts_present": list_items(
            observed_input_summary.get("helper_artifacts_present")
        ),
        "denominator_boundary": (
            "Timeline denominators are node/signal visibility counts only; public "
            "semantic percentages require source-family-local corpus, coverage, "
            "and annotation denominators."
        ),
    }
    refs = _brief_refs(nodes, lane_episode_cards)
    return [
        {
            "brief_id": "section-brief-fpp-"
            + stable_hash(timeline_context.get("path"), len(nodes))[:12],
            "section_key": "fact-policy-public-interaction-timeline",
            "section_role": "Bounded chronology for fact/policy/public interaction context.",
            "source_artifact_path": maybe_text(timeline_context.get("path")),
            "refs": refs,
            "evidence_refs": refs,
            "claim_strength": "bounded-descriptive-context-only",
            "denominator": denominators,
            "limitations": limitations,
            "candidate_section_claims": [
                "Fact/policy-side lane episode cards and public/media lane episode cards were visible in the same timeline windows listed in the cited nodes."
            ],
            "if_not_used_report_boundary": (
                "Omit interaction framing and keep fact/policy chronology separate "
                "from public/media sample discussion."
            ),
            "report_use_requires": [
                "lane-episode-cards",
                "finding-record",
                "evidence-bundle",
                "round-synthesis",
                "report-section-draft",
                "report-basis citation",
            ],
        }
    ]


def load_agent_section_briefs(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
) -> list[dict[str, Any]]:
    try:
        payload = query_reporting_objects(
            run_dir,
            object_kind="agent-section-brief",
            run_id=run_id,
            round_id=round_id,
            limit=100,
        )
    except Exception:
        return []
    return [
        item
        for item in list_items(payload.get("objects"))
        if isinstance(item, dict)
    ]


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def excerpt_text(text: str, limit: int = 280) -> str:
    normalized = maybe_text(text)
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3].rstrip() + "..."


def legacy_reporting_basis_count(report_basis_freeze: dict[str, Any]) -> int:
    frozen_basis = (
        report_basis_freeze.get("frozen_basis", {})
        if isinstance(report_basis_freeze.get("frozen_basis"), dict)
        else {}
    )
    count = 0
    for key in (
        "selected_coverages",
        "coverages",
        "empirical_support_coverages",
        "verification_routes",
        "formal_public_links",
        "representation_gaps",
        "diffusion_edges",
    ):
        rows = report_basis_freeze.get(key, frozen_basis.get(key, []))
        if isinstance(rows, list):
            count += len(rows)
    return count


def query_round_council_objects(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
    object_kind: str,
    limit: int = 50,
) -> list[dict[str, Any]]:
    try:
        payload = query_council_objects(
            run_dir,
            object_kind=object_kind,
            run_id=run_id,
            round_id=round_id,
            limit=limit,
        )
    except Exception:
        return []
    rows = payload.get("objects", []) if isinstance(payload.get("objects"), list) else []
    return [row for row in rows if isinstance(row, dict)]


def load_reporting_basis_objects(run_dir: Path, *, run_id: str, round_id: str) -> dict[str, list[dict[str, Any]]]:
    query_kinds_by_basis_kind = {
        "finding-record": "finding",
        "evidence-bundle": "evidence-bundle",
        "proposal": "proposal",
        "readiness-opinion": "readiness-opinion",
        "challenge": "challenge",
        "review-comment": "review-comment",
    }
    return {
        basis_kind: query_round_council_objects(
            run_dir,
            run_id=run_id,
            round_id=round_id,
            object_kind=query_kind,
        )
        for basis_kind, query_kind in query_kinds_by_basis_kind.items()
    }


def object_id_for(object_kind: str, row: dict[str, Any]) -> str:
    for field_name in (
        "finding_id",
        "bundle_id",
        "proposal_id",
        "opinion_id",
        "challenge_id",
        "comment_id",
        "ticket_id",
        "id",
    ):
        value = maybe_text(row.get(field_name))
        if value:
            return value
    return object_kind + "-" + stable_hash(object_kind, row.get("summary"), row.get("title"))[:12]


def object_summary(row: dict[str, Any]) -> str:
    for field_name in ("summary", "title", "rationale", "opinion_text", "challenge_statement", "comment_text"):
        value = maybe_text(row.get(field_name))
        if value:
            return value
    return "DB council object without a compact summary."


def evidence_refs_for(row: dict[str, Any]) -> list[str]:
    return unique_texts(list_items(row.get("evidence_refs")))


def build_evidence_index(
    *,
    selected_evidence_refs: list[str],
    council_basis: dict[str, list[dict[str, Any]]],
    max_items: int = 40,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, evidence_ref in enumerate(selected_evidence_refs, start=1):
        rows.append(
            {
                "evidence_id": f"frozen-evidence-ref-{index:03d}",
                "object_kind": "evidence-ref",
                "object_id": evidence_ref,
                "summary": "Frozen DB report-basis evidence reference.",
                "evidence_refs": [evidence_ref],
                "basis_role": "frozen-report-basis",
                "source_plane": "deliberation",
                "report_use": "citation-index",
            }
        )
    for object_kind in ("finding-record", "evidence-bundle", "proposal", "readiness-opinion", "challenge", "review-comment"):
        for row in council_basis.get(object_kind, []):
            object_id = object_id_for(object_kind, row)
            refs = evidence_refs_for(row)
            if object_kind in {"finding-record", "evidence-bundle"} and not refs:
                continue
            rows.append(
                {
                    "evidence_id": f"{object_kind}:{object_id}",
                    "object_kind": object_kind,
                    "object_id": object_id,
                    "summary": object_summary(row),
                    "evidence_refs": refs,
                    "basis_object_ids": unique_texts(list_items(row.get("basis_object_ids"))),
                    "source_signal_ids": unique_texts(list_items(row.get("source_signal_ids"))),
                    "basis_role": (
                        "investigator-evidence"
                        if object_kind in {"finding-record", "evidence-bundle"}
                        else "council-context"
                    ),
                    "source_plane": "deliberation",
                    "report_use": (
                        "report-basis-candidate"
                        if object_kind in {"finding-record", "evidence-bundle"}
                        else "audit-context"
                    ),
                }
            )
    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = "|".join(
            [
                maybe_text(row.get("object_kind")),
                maybe_text(row.get("object_id")),
                ",".join(evidence_refs_for(row)),
            ]
        )
        if key and key not in deduped:
            deduped[key] = row
    return list(deduped.values())[:max_items]


def build_key_findings_from_council_basis(
    council_basis: dict[str, list[dict[str, Any]]],
    max_findings: int,
    *,
    selected_evidence_refs: list[str],
    selected_basis_object_ids: list[str],
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    selected_ref_lookup = set(unique_texts(selected_evidence_refs))
    selected_basis_lookup = set(unique_texts(selected_basis_object_ids))
    for row in council_basis.get("finding-record", []):
        refs = evidence_refs_for(row)
        if not refs:
            continue
        finding_id = object_id_for("finding-record", row)
        if finding_id not in selected_basis_lookup and not selected_ref_lookup.intersection(refs):
            continue
        findings.append(
            {
                "finding_id": finding_id,
                "title": maybe_text(row.get("title")) or f"Finding {len(findings) + 1}",
                "summary": object_summary(row),
                "finding_kind": maybe_text(row.get("finding_kind")) or "finding",
                "agent_role": maybe_text(row.get("agent_role")),
                "evidence_refs": refs,
                "basis_object_ids": unique_texts(list_items(row.get("basis_object_ids"))),
            }
        )
        if len(findings) >= max(0, max_findings):
            break
    return findings


def build_open_risks(
    *,
    report_basis_freeze: dict[str, Any],
    supervisor_state: dict[str, Any],
    readiness: dict[str, Any],
) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    readiness_status = maybe_text(readiness.get("readiness_status"))
    supervisor_status = maybe_text(supervisor_state.get("supervisor_status"))
    remaining_risks = report_basis_freeze.get("remaining_risks", []) if isinstance(report_basis_freeze.get("remaining_risks"), list) else []
    for index, risk in enumerate(remaining_risks, start=1):
        if not isinstance(risk, dict):
            continue
        results.append(
            {
                "risk_id": maybe_text(risk.get("action_id")) or f"report-basis-risk-{index:03d}",
                "risk_type": maybe_text(risk.get("action_kind")) or "investigation",
                "priority": maybe_text(risk.get("priority")) or "medium",
                "summary": maybe_text(risk.get("reason")) or "Promotion basis still carries unresolved risk.",
            }
        )
    unresolved_constraints: list[dict[str, Any]] = []
    for source in (report_basis_freeze, readiness):
        for constraint in list_items(source.get("unresolved_challenger_constraints")):
            if isinstance(constraint, dict):
                unresolved_constraints.append(constraint)
    for index, constraint in enumerate(unresolved_constraints, start=1):
        results.append(
            {
                "risk_id": maybe_text(constraint.get("constraint_id"))
                or f"challenger-constraint-{index:03d}",
                "risk_type": "challenger-constraint",
                "priority": "high",
                "summary": maybe_text(constraint.get("comment_text"))
                or "A challenger constraint requires explicit disposition before reporting handoff.",
            }
        )
    for index, violation in enumerate(
        list_items(report_basis_freeze.get("lead_basis_constraint_violations")),
        start=1,
    ):
        if not isinstance(violation, dict):
            continue
        results.append(
            {
                "risk_id": maybe_text(violation.get("violation_id"))
                or f"lead-basis-constraint-{index:03d}",
                "risk_type": "lead-basis-constraint",
                "priority": "high",
                "summary": (
                    "An explicit lead-basis declaration conflicts with a structural "
                    "claim or challenger constraint."
                ),
            }
        )
    for index, violation in enumerate(
        list_items(report_basis_freeze.get("report_claim_structural_violations")),
        start=1,
    ):
        if not isinstance(violation, dict):
            continue
        results.append(
            {
                "risk_id": maybe_text(violation.get("violation_id"))
                or f"report-claim-structural-{index:03d}",
                "risk_type": maybe_text(violation.get("violation_kind"))
                or "report-claim-structural",
                "priority": "high",
                "summary": (
                    "An explicit report-claim declaration is missing structural "
                    "fields or a challenger disposition link."
                ),
            }
        )
    if supervisor_status and supervisor_status != "reporting-ready":
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
    if readiness_status != "ready":
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


def build_recommended_next_actions(
    supervisor_state: dict[str, Any],
    *,
    open_risks: list[dict[str, str]] | None = None,
    reporting_blocker_hints: list[str] | None = None,
) -> list[dict[str, str]]:
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
    if recommendations:
        return recommendations
    for risk in (open_risks or [])[:4]:
        if not isinstance(risk, dict):
            continue
        summary = maybe_text(risk.get("summary"))
        if not summary:
            continue
        priority = maybe_text(risk.get("priority")) or "medium"
        risk_type = maybe_text(risk.get("risk_type")) or "reporting-blocker"
        recommendations.append(
            {
                "assigned_role": maybe_text(risk.get("assigned_role")) or "moderator",
                "objective": f"Resolve or explicitly carry forward: {summary}",
                "reason": (
                    "Reporting is held because this "
                    f"{risk_type} remains open (priority={priority})."
                ),
            }
        )
    if recommendations:
        return recommendations
    for hint in (reporting_blocker_hints or [])[:4]:
        summary = maybe_text(hint)
        if not summary:
            continue
        recommendations.append(
            {
                "assigned_role": "moderator",
                "objective": f"Address reporting blocker: {summary}",
                "reason": "Reporting is held until the blocker is resolved, accepted, or scoped into a follow-up round.",
            }
        )
    return recommendations


def recommended_sections(reporting_ready: bool) -> list[str]:
    if reporting_ready:
        return ["executive-summary", "role-reports", "evidence-basis", "residual-risks", "audit-trace"]
    return ["gating-status", "open-risks", "next-round-plan", "audit-trace"]


def build_uncertainty_register(
    *,
    open_risks: list[dict[str, str]],
    reporting_blocker_hints: list[str],
    evidence_index: list[dict[str, Any]],
    accepted_limitations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    register: list[dict[str, Any]] = []
    for index, limitation in enumerate(accepted_limitations, start=1):
        if not isinstance(limitation, dict):
            continue
        summary = maybe_text(limitation.get("summary"))
        if not summary:
            continue
        register.append(
            {
                "uncertainty_id": maybe_text(limitation.get("limitation_id"))
                or f"accepted-limitation-{index:03d}",
                "uncertainty_type": "accepted-limitation",
                "summary": summary,
                "report_treatment": maybe_text(limitation.get("report_treatment"))
                or "Carry as an explicit limitation.",
            }
        )
    for index, risk in enumerate(open_risks, start=1):
        summary = maybe_text(risk.get("summary")) if isinstance(risk, dict) else ""
        if not summary:
            continue
        register.append(
            {
                "uncertainty_id": f"open-risk-{index:03d}",
                "uncertainty_type": maybe_text(risk.get("risk_type")) or "open-risk",
                "summary": summary,
                "report_treatment": "Carry as unresolved risk or scope a follow-up evidence request.",
            }
        )
    for index, hint in enumerate(reporting_blocker_hints, start=1):
        summary = maybe_text(hint)
        if not summary:
            continue
        register.append(
            {
                "uncertainty_id": f"reporting-blocker-{index:03d}",
                "uncertainty_type": "reporting-blocker",
                "summary": summary,
                "report_treatment": "Do not present as resolved evidence until moderator or report_basis explicitly addresses it.",
            }
        )
    if not any(row.get("object_kind") in {"finding-record", "evidence-bundle"} for row in evidence_index):
        register.append(
            {
                "uncertainty_id": "missing-investigator-basis-001",
                "uncertainty_type": "report-basis-gap",
                "summary": "No DB finding-record or evidence-bundle is available for direct report citation.",
                "report_treatment": "Use frozen evidence refs only as citation index until an investigator or report editor cites them through DB basis objects.",
            }
        )
    return register[:8]


def build_residual_disputes(
    *,
    reporting_blockers: list[str],
    rejected_proposal_ids: list[str],
    rejected_opinion_ids: list[str],
    open_risks: list[dict[str, str]],
    unresolved_challenges: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    disputes: list[dict[str, Any]] = []
    for challenge in unresolved_challenges:
        if not isinstance(challenge, dict):
            continue
        summary = maybe_text(challenge.get("summary"))
        if not summary:
            continue
        disputes.append(
            {
                "dispute_id": maybe_text(challenge.get("challenge_id"))
                or "unresolved-challenge",
                "object_kind": "challenger-constraint",
                "object_id": maybe_text(challenge.get("constraint_id")),
                "summary": summary,
                "status": "unresolved",
            }
        )
    for index, blocker in enumerate(reporting_blockers, start=1):
        summaries = reporting_blocker_summaries([blocker])
        disputes.append(
            {
                "dispute_id": f"reporting-blocker-{index:03d}",
                "object_kind": "reporting-blocker",
                "object_id": blocker,
                "summary": summaries[0] if summaries else blocker,
                "status": "open",
            }
        )
    for proposal_id in rejected_proposal_ids:
        disputes.append(
            {
                "dispute_id": f"proposal-veto:{proposal_id}",
                "object_kind": "proposal",
                "object_id": proposal_id,
                "summary": "Council proposal is rejected or vetoed for current publication posture.",
                "status": "unresolved",
            }
        )
    for opinion_id in rejected_opinion_ids:
        disputes.append(
            {
                "dispute_id": f"readiness-veto:{opinion_id}",
                "object_kind": "readiness-opinion",
                "object_id": opinion_id,
                "summary": "Readiness opinion blocks or qualifies final publication.",
                "status": "unresolved",
            }
        )
    for risk in open_risks:
        summary = maybe_text(risk.get("summary")) if isinstance(risk, dict) else ""
        if summary:
            disputes.append(
                {
                    "dispute_id": maybe_text(risk.get("risk_id")) or "open-risk",
                    "object_kind": maybe_text(risk.get("risk_type")) or "risk",
                    "object_id": maybe_text(risk.get("risk_id")),
                    "summary": summary,
                    "status": "open",
                }
            )
    return disputes[:8]


def build_policy_recommendations(
    *,
    reporting_ready: bool,
    recommended_next_actions: list[dict[str, str]],
    uncertainty_register: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return []


def build_packets(
    *,
    run_id: str,
    round_id: str,
    reporting_ready: bool,
    handoff_status: str,
    report_basis_status: str,
    readiness_status: str,
    supervisor_status: str,
    reporting_blockers: list[str],
    selected_basis_object_ids: list[str],
    selected_evidence_refs: list[str],
    supporting_proposal_ids: list[str],
    rejected_proposal_ids: list[str],
    supporting_opinion_ids: list[str],
    rejected_opinion_ids: list[str],
    council_input_counts: dict[str, Any],
    key_findings: list[dict[str, Any]],
    open_risks: list[dict[str, str]],
    recommended_next_actions: list[dict[str, str]],
    council_basis: dict[str, list[dict[str, Any]]],
    reporting_blocker_hints: list[str],
    accepted_limitations: list[dict[str, Any]],
    unresolved_challenges: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    evidence_index = build_evidence_index(
        selected_evidence_refs=selected_evidence_refs,
        council_basis=council_basis,
    )
    uncertainty_register = build_uncertainty_register(
        open_risks=open_risks,
        reporting_blocker_hints=reporting_blocker_hints,
        evidence_index=evidence_index,
        accepted_limitations=accepted_limitations,
    )
    residual_disputes = build_residual_disputes(
        reporting_blockers=reporting_blockers,
        rejected_proposal_ids=rejected_proposal_ids,
        rejected_opinion_ids=rejected_opinion_ids,
        open_risks=open_risks,
        unresolved_challenges=unresolved_challenges,
    )
    policy_recommendations = build_policy_recommendations(
        reporting_ready=reporting_ready,
        recommended_next_actions=recommended_next_actions,
        uncertainty_register=uncertainty_register,
    )
    packet_suffix = stable_hash(run_id, round_id, handoff_status, report_basis_status)[:12]
    evidence_packet = {
        "packet_id": f"evidence-packet-{packet_suffix}",
        "packet_kind": "decision-maker-report-evidence-packet",
        "source": "db-canonical-report-basis",
        "selected_basis_object_ids": selected_basis_object_ids,
        "selected_evidence_refs": selected_evidence_refs,
        "evidence_index": evidence_index,
        "key_findings": key_findings,
        "basis_object_counts": {
            object_kind: len(rows)
            for object_kind, rows in sorted(council_basis.items())
        },
        "caveats": [
            "Helper and fallback outputs are not direct report_basis unless cited through DB council/reporting objects.",
            "Frozen evidence refs identify citation candidates; report conclusions require finding, evidence bundle, proposal, or report section basis.",
        ],
        "report_basis_input_policy": report_basis_input_policy(),
    }
    decision_packet = {
        "packet_id": f"decision-packet-{packet_suffix}",
        "packet_kind": "moderator-decision-memo-packet",
        "handoff_status": handoff_status,
        "reporting_ready": reporting_ready,
        "report_basis_status": report_basis_status,
        "readiness_status": readiness_status,
        "supervisor_status": supervisor_status,
        "reporting_blockers": reporting_blockers,
        "supporting_proposal_ids": supporting_proposal_ids,
        "rejected_proposal_ids": rejected_proposal_ids,
        "supporting_opinion_ids": supporting_opinion_ids,
        "rejected_opinion_ids": rejected_opinion_ids,
        "council_input_counts": council_input_counts,
        "open_risks": open_risks,
        "recommended_next_actions": recommended_next_actions,
        "residual_disputes": residual_disputes,
        "accepted_limitations": accepted_limitations,
        "unresolved_challenges": unresolved_challenges,
        "report_basis_input_policy": report_basis_input_policy(),
    }
    report_packet = {
        "packet_id": f"report-packet-{packet_suffix}",
        "packet_kind": "decision-maker-policy-report-packet",
        "report_type": "decision-maker-environmental-policy-report",
        "recommended_sections": [
            {"section_key": "decision-question", "required_basis": "moderator-defined question or decision memo"},
            {"section_key": "regional-and-policy-context", "required_basis": "DB evidence bundle or report section draft"},
            {"section_key": "evidence-sources-and-scope", "required_basis": "evidence_packet.evidence_index"},
            {"section_key": "key-findings", "required_basis": "finding-record or report-section-draft"},
            {"section_key": "options-and-tradeoffs", "required_basis": "proposal or report-section-draft"},
            {"section_key": "risks-and-uncertainties", "required_basis": "uncertainty_register"},
            {"section_key": "recommendations", "required_basis": "policy_recommendations with evidence refs"},
            {"section_key": "remaining-disputes", "required_basis": "residual_disputes"},
            {"section_key": "citation-index", "required_basis": "evidence_packet.evidence_index"},
        ],
        "uncertainty_register": uncertainty_register,
        "residual_disputes": residual_disputes,
        "accepted_limitations": accepted_limitations,
        "unresolved_challenges": unresolved_challenges,
        "policy_recommendations": policy_recommendations,
        "report_basis_input_policy": report_basis_input_policy(),
    }
    return evidence_packet, decision_packet, report_packet


def materialize_reporting_handoff_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    report_basis_path: str,
    readiness_path: str,
    board_brief_path: str,
    supervisor_state_path: str,
    output_path: str,
    max_findings: int,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    report_basis_file = resolve_path(run_dir_path, report_basis_path, f"report_basis/frozen_report_basis_{round_id}.json")
    output_file = resolve_path(run_dir_path, output_path, f"reporting/reporting_handoff_{round_id}.json")

    warnings: list[dict[str, Any]] = []
    report_basis_context = load_report_basis_freeze_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        report_basis_path=report_basis_path,
    )
    report_basis_payload = (
        report_basis_context.get("payload")
        if isinstance(report_basis_context.get("payload"), dict)
        else None
    )
    if not isinstance(report_basis_payload, dict):
        warnings.append(
            {
                "code": "missing-report-basis-freeze",
                "message": (
                    "No report-basis DB record was found for "
                    f"{report_basis_file}; artifact exists but is orphaned from the deliberation plane."
                    if bool(report_basis_context.get("artifact_present"))
                    else (
                        "No report-basis artifact or DB record was found "
                        f"at {report_basis_file}."
                    )
                ),
            }
        )
        report_basis_freeze = {
            "report_basis_status": "withheld",
            "selected_coverages": [],
            "selected_evidence_refs": [],
            "remaining_risks": [],
        }
    else:
        report_basis_freeze = report_basis_payload
    basis_round_id = maybe_text(report_basis_freeze.get("round_id")) or round_id
    readiness_file = resolve_path(run_dir_path, readiness_path, f"reporting/round_readiness_{basis_round_id}.json")
    board_brief_file = resolve_path(run_dir_path, board_brief_path, f"board/board_brief_{basis_round_id}.md")
    supervisor_file = resolve_path(run_dir_path, supervisor_state_path, f"runtime/supervisor_state_{basis_round_id}.json")
    readiness_context = load_round_readiness_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=basis_round_id,
        readiness_path=readiness_path,
    )
    readiness_payload = (
        readiness_context.get("payload")
        if isinstance(readiness_context.get("payload"), dict)
        else None
    )
    if not isinstance(readiness_payload, dict):
        warnings.append(
            {
                "code": "missing-readiness",
                "message": (
                    "No round readiness DB assessment was found for "
                    f"{readiness_file}; artifact exists but is orphaned from the deliberation plane."
                    if bool(readiness_context.get("artifact_present"))
                    else (
                        "No round readiness artifact or DB assessment was found "
                        f"at {readiness_file}."
                    )
                ),
            }
        )
        readiness = {"readiness_status": "blocked", "gate_reasons": []}
    else:
        readiness = readiness_payload
    supervisor_context = load_supervisor_state_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=basis_round_id,
        supervisor_state_path=supervisor_state_path,
    )
    supervisor_state_payload = (
        supervisor_context.get("payload")
        if isinstance(supervisor_context.get("payload"), dict)
        else None
    )
    if not isinstance(supervisor_state_payload, dict):
        warnings.append(
            {
                "code": "missing-supervisor-state",
                "message": (
                    "No supervisor DB snapshot was found for "
                    f"{supervisor_file}; artifact exists but is orphaned from the governed-execution control plane."
                    if bool(supervisor_context.get("artifact_present"))
                    else (
                        "No supervisor snapshot artifact or DB record was found "
                        f"at {supervisor_file}."
                    )
                ),
            }
        )
        supervisor_state = {"supervisor_status": "unavailable", "top_actions": [], "operator_notes": []}
    else:
        supervisor_state = supervisor_state_payload
    report_basis_transition_request_id = maybe_text(
        report_basis_freeze.get("transition_request_id")
    )
    supervisor_transition_request_id = maybe_text(
        supervisor_state.get("adopted_transition_request_id")
    )
    supervisor_freshness_status = (
        "fresh"
        if (
            report_basis_transition_request_id
            and supervisor_transition_request_id
            and report_basis_transition_request_id == supervisor_transition_request_id
        )
        else "stale"
        if report_basis_transition_request_id and supervisor_transition_request_id
        else "untracked"
    )
    if supervisor_freshness_status == "stale":
        stale_warning = {
            "code": "stale-supervisor-state",
            "message": (
                "Supervisor snapshot adopted transition request "
                f"`{supervisor_transition_request_id}`, but report-basis freeze uses "
                f"`{report_basis_transition_request_id}`. Rerun supervise-round before "
                "materializing reporting handoff."
            ),
        }
        warnings.append(stale_warning)
        operator_notes = (
            list(supervisor_state.get("operator_notes", []))
            if isinstance(supervisor_state.get("operator_notes"), list)
            else []
        )
        operator_notes.append(stale_warning["message"])
        supervisor_state = {
            **supervisor_state,
            "supervisor_status": "stale-controller",
            "reporting_ready": False,
            "reporting_handoff_status": "investigation-open",
            "operator_notes": operator_notes,
        }
    board_brief_text = load_text_if_exists(board_brief_file)
    contract_fields = reporting_contract_fields_from_payload(
        report_basis_payload,
        fallback_payload=readiness_payload,
        observed_inputs_overrides={
            "report_basis_artifact_present": bool(
                report_basis_context.get("artifact_present")
            ),
            "report_basis_present": bool(report_basis_context.get("payload_present")),
            "readiness_artifact_present": bool(
                readiness_context.get("artifact_present")
            ),
            "readiness_present": bool(readiness_context.get("payload_present")),
            "board_brief_artifact_present": board_brief_file.exists(),
            "board_brief_present": bool(maybe_text(board_brief_text)),
            "supervisor_state_artifact_present": supervisor_file.exists(),
            "supervisor_state_present": bool(supervisor_context.get("payload_present")),
        },
        field_overrides={
            "report_basis_source": maybe_text(report_basis_context.get("source"))
            or "missing-report-basis",
            "readiness_source": (
                maybe_text(readiness_context.get("source"))
                or "missing-readiness"
            ),
            "board_brief_source": (
                "board-brief-artifact"
                if board_brief_file.exists()
                else "missing-board-brief"
            ),
            "supervisor_state_source": (
                maybe_text(supervisor_context.get("source"))
                or "missing-supervisor-state"
            ),
        },
    )

    gate_state = reporting_gate_state(
        report_basis_status=maybe_text(report_basis_freeze.get("report_basis_status")) or "withheld",
        readiness_status=maybe_text(readiness.get("readiness_status")) or "blocked",
        supervisor_status=maybe_text(supervisor_state.get("supervisor_status")) or "unavailable",
        require_supervisor=True,
    )
    report_basis_status = maybe_text(gate_state.get("report_basis_status")) or "withheld"
    readiness_status = maybe_text(gate_state.get("readiness_status")) or "blocked"
    supervisor_status = maybe_text(gate_state.get("supervisor_status")) or "unavailable"
    handoff_status = maybe_text(gate_state.get("handoff_status")) or "investigation-open"
    reporting_ready = bool(gate_state.get("reporting_ready"))
    reporting_blockers = unique_texts(
        gate_state.get("reporting_blockers", [])
        if isinstance(gate_state.get("reporting_blockers"), list)
        else []
    )
    unresolved_challenger_constraints = [
        constraint
        for source in (report_basis_freeze, readiness)
        for constraint in list_items(source.get("unresolved_challenger_constraints"))
        if isinstance(constraint, dict)
    ]
    basis_use_constraints = [
        constraint
        for source in (report_basis_freeze, readiness)
        for constraint in list_items(source.get("basis_use_constraints"))
        if isinstance(constraint, dict)
    ]
    accepted_limitations = accepted_limitations_from_constraints(basis_use_constraints)
    unresolved_challenges = unresolved_challenges_from_constraints(
        unresolved_challenger_constraints
    )
    lead_basis_constraint_violations = [
        violation
        for violation in list_items(
            report_basis_freeze.get("lead_basis_constraint_violations")
        )
        if isinstance(violation, dict)
    ]
    report_claim_structural_violations = [
        violation
        for violation in list_items(
            report_basis_freeze.get("report_claim_structural_violations")
        )
        if isinstance(violation, dict)
    ]
    if (
        unresolved_challenger_constraints
        or lead_basis_constraint_violations
        or report_claim_structural_violations
    ):
        reporting_ready = False
        handoff_status = "investigation-open"
        blocker_updates = []
        if unresolved_challenger_constraints:
            blocker_updates.append("unresolved-challenger-constraints")
        if lead_basis_constraint_violations:
            blocker_updates.append("lead-basis-constraint-violations")
        if report_claim_structural_violations:
            blocker_updates.append("report-claim-structural-violations")
        reporting_blockers = unique_texts(reporting_blockers + blocker_updates)
    reporting_blocker_hints = reporting_blocker_summaries(reporting_blockers)

    selected_basis_object_ids = unique_texts(
        report_basis_freeze.get("selected_basis_object_ids", [])
        if isinstance(report_basis_freeze.get("selected_basis_object_ids"), list)
        else []
    )
    selected_evidence_refs = unique_texts(
        report_basis_freeze.get("selected_evidence_refs", [])
        if isinstance(report_basis_freeze.get("selected_evidence_refs"), list)
        else []
    )
    council_basis = load_reporting_basis_objects(
        run_dir_path,
        run_id=run_id,
        round_id=basis_round_id,
    )
    key_findings = build_key_findings_from_council_basis(
        council_basis,
        max_findings,
        selected_evidence_refs=selected_evidence_refs,
        selected_basis_object_ids=selected_basis_object_ids,
    )
    ignored_legacy_basis_count = legacy_reporting_basis_count(report_basis_freeze)
    if ignored_legacy_basis_count:
        warnings.append(
            {
                "code": "legacy-optional-analysis-reporting-basis-ignored",
                "message": (
                    "Ignored legacy claim/coverage, routing, linkage, gap, or diffusion "
                    f"basis rows ({ignored_legacy_basis_count}) while materializing reporting handoff."
                ),
            }
        )
    open_risks = build_open_risks(report_basis_freeze=report_basis_freeze, supervisor_state=supervisor_state, readiness=readiness)
    next_actions = build_recommended_next_actions(
        supervisor_state,
        open_risks=open_risks,
        reporting_blocker_hints=reporting_blocker_hints,
    )
    claim_gap_cards_context = load_claim_gap_action_cards(
        run_dir_path,
        basis_round_id,
    )
    claim_gap_action_cards = [
        card
        for card in list_items(claim_gap_cards_context.get("action_cards"))
        if isinstance(card, dict)
    ]
    interaction_timeline_context = load_interaction_timeline(
        run_dir_path,
        basis_round_id,
    )
    acquisition_checkpoint_context = load_analysis_item_artifact(
        run_dir_path,
        basis_round_id,
        file_stem="acquisition_checkpoints",
        item_key="acquisition_checkpoints",
    )
    theme_sufficiency_context = load_analysis_item_artifact(
        run_dir_path,
        basis_round_id,
        file_stem="theme_sufficiency_review",
        item_key="theme_sufficiency_reviews",
    )
    interaction_nodes = [
        node
        for node in list_items(interaction_timeline_context.get("interaction_nodes"))
        if isinstance(node, dict)
    ]
    lane_episode_cards = [
        card
        for card in list_items(interaction_timeline_context.get("lane_episode_cards"))
        if isinstance(card, dict)
    ]
    acquisition_checkpoints = [
        item
        for item in list_items(acquisition_checkpoint_context.get("items"))
        if isinstance(item, dict)
    ]
    theme_sufficiency_reviews = [
        item
        for item in list_items(theme_sufficiency_context.get("items"))
        if isinstance(item, dict)
    ]
    theme_progress_reviews = [
        item
        for item in list_items(
            load_json_if_exists(theme_sufficiency_context.get("path")).get("theme_progress_reviews")
            if maybe_text(theme_sufficiency_context.get("path"))
            else []
        )
        if isinstance(item, dict)
    ]
    council_programs = query_round_council_objects(
        run_dir_path,
        run_id=run_id,
        round_id=basis_round_id,
        object_kind="council-investigation-program",
        limit=20,
    )
    if not council_programs:
        try:
            payload = query_council_objects(
                run_dir_path,
                object_kind="council-investigation-program",
                run_id=run_id,
                limit=20,
            )
            council_programs = [
                item
                for item in list_items(payload.get("objects"))
                if isinstance(item, dict)
            ]
        except Exception:
            council_programs = []
    agent_section_briefs = load_agent_section_briefs(
        run_dir_path,
        run_id=run_id,
        round_id=basis_round_id,
    )
    interaction_section_briefs = build_interaction_section_briefs(interaction_timeline_context)
    section_briefs = [*agent_section_briefs, *interaction_section_briefs]
    supporting_proposal_ids = unique_texts(
        report_basis_freeze.get("supporting_proposal_ids", [])
        if isinstance(report_basis_freeze.get("supporting_proposal_ids"), list)
        else []
    )
    rejected_proposal_ids = unique_texts(
        report_basis_freeze.get("rejected_proposal_ids", [])
        if isinstance(report_basis_freeze.get("rejected_proposal_ids"), list)
        else []
    )
    supporting_opinion_ids = unique_texts(
        report_basis_freeze.get("supporting_opinion_ids", [])
        if isinstance(report_basis_freeze.get("supporting_opinion_ids"), list)
        else []
    )
    rejected_opinion_ids = unique_texts(
        report_basis_freeze.get("rejected_opinion_ids", [])
        if isinstance(report_basis_freeze.get("rejected_opinion_ids"), list)
        else []
    )
    council_input_counts = (
        report_basis_freeze.get("council_input_counts", {})
        if isinstance(report_basis_freeze.get("council_input_counts"), dict)
        else {}
    )
    evidence_packet, decision_packet, report_packet = build_packets(
        run_id=run_id,
        round_id=round_id,
        reporting_ready=reporting_ready,
        handoff_status=handoff_status,
        report_basis_status=report_basis_status,
        readiness_status=readiness_status,
        supervisor_status=supervisor_status,
        reporting_blockers=reporting_blockers,
        selected_basis_object_ids=selected_basis_object_ids,
        selected_evidence_refs=selected_evidence_refs,
        supporting_proposal_ids=supporting_proposal_ids,
        rejected_proposal_ids=rejected_proposal_ids,
        supporting_opinion_ids=supporting_opinion_ids,
        rejected_opinion_ids=rejected_opinion_ids,
        council_input_counts=council_input_counts,
        key_findings=key_findings,
        open_risks=open_risks,
        recommended_next_actions=next_actions,
        council_basis=council_basis,
        reporting_blocker_hints=reporting_blocker_hints,
        accepted_limitations=accepted_limitations,
        unresolved_challenges=unresolved_challenges,
    )
    report_packet["claim_gap_action_cards"] = claim_gap_action_cards
    report_packet["claim_gap_action_card_policy"] = {
        "artifact_path": maybe_text(claim_gap_cards_context.get("path")),
        "present": bool(claim_gap_cards_context.get("present")),
        "advisory_semantics": (
            "Claim-gap action cards expose optional report-boundary and recovery "
            "cues. They are not report basis unless carried by council or "
            "reporting objects, and they do not rank, schedule, or execute skills."
        ),
    }
    report_packet["interaction_timeline_nodes"] = interaction_nodes
    report_packet["lane_episode_cards"] = lane_episode_cards
    report_packet["lane_episode_card_policy"] = {
        "artifact_path": maybe_text(interaction_timeline_context.get("path")),
        "present": bool(lane_episode_cards),
        "advisory_semantics": (
            "Lane episode cards are the required pre-composition layer for interaction "
            "timeline nodes. They organize each lane before report-editor synthesis and "
            "do not prove causality, representativeness, or policy effectiveness."
        ),
    }
    report_packet["interaction_timeline_policy"] = {
        "artifact_path": maybe_text(interaction_timeline_context.get("path")),
        "present": bool(interaction_timeline_context.get("present")),
        "advisory_semantics": (
            "Interaction timeline nodes are descriptive helper context. They are "
            "not report basis unless carried by council or reporting objects, and "
            "they do not prove causality, policy impact, public response attribution, "
            "representativeness, or evidence absence."
        ),
    }
    report_packet["acquisition_checkpoints"] = acquisition_checkpoints
    report_packet["acquisition_checkpoint_policy"] = {
        "artifact_path": maybe_text(acquisition_checkpoint_context.get("path")),
        "present": bool(acquisition_checkpoints),
        "advisory_semantics": (
            "Acquisition checkpoints are lightweight claim-impact notes only. "
            "They appear when acquisition state can change claim strength, "
            "source-limit rationale, report downgrade, or recovery choice; "
            "they are not per-tool-call forms or findings."
        ),
    }
    report_packet["theme_sufficiency_reviews"] = theme_sufficiency_reviews
    report_packet["theme_progress_reviews"] = theme_progress_reviews
    report_packet["council_investigation_programs"] = council_programs
    report_packet["theme_sufficiency_review_policy"] = {
        "artifact_path": maybe_text(theme_sufficiency_context.get("path")),
        "present": bool(theme_sufficiency_reviews),
        "advisory_semantics": (
            "Theme sufficiency review states which claim slots can be supported, "
            "downgraded, or lack basis. It is not a runtime truth mechanism and "
            "does not replace council objects, report basis, gate, or freeze."
        ),
    }
    report_packet["theme_progress_review_policy"] = {
        "artifact_path": maybe_text(theme_sufficiency_context.get("path")),
        "present": bool(theme_progress_reviews),
        "advisory_semantics": (
            "Theme progress review recommends disposition only. State changes, "
            "supplemental rounds, and report use still require council objects, "
            "moderator synthesis, readiness opinion, report-basis gate, or "
            "transition approval."
        ),
    }
    report_packet["council_program_policy"] = {
        "present": bool(council_programs),
        "advisory_semantics": (
            "Council investigation programs organize agenda questions, active "
            "themes, responsibility boundaries, exits, and downgrades. They are "
            "not source plans, query plans, task queues, or schedulers."
        ),
    }
    report_packet["section_briefs"] = section_briefs
    report_packet["section_brief_policy"] = {
        "source": "db-agent-section-briefs-plus-carried-helper-fallbacks",
        "present": bool(section_briefs),
        "agent_section_brief_count": len(agent_section_briefs),
        "derived_interaction_section_brief_count": len(interaction_section_briefs),
        "advisory_semantics": (
            "Agent section briefs are report-editor inputs authored or adopted by "
            "roles before synthesis. Derived helper fallbacks stay advisory. Briefs "
            "do not create a parallel report path or replace frozen/reporting basis."
        ),
    }
    board_excerpt = excerpt_text(board_brief_text)
    handoff_id = "reporting-handoff-" + stable_hash(run_id, round_id, handoff_status, report_basis_status)[:12]

    wrapper = {
        "schema_version": "e1.0",
        "skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "basis_round_id": basis_round_id,
        "handoff_id": handoff_id,
        "handoff_status": handoff_status,
        "reporting_ready": reporting_ready,
        "reporting_blockers": reporting_blockers,
        "report_basis_status": report_basis_status,
        "report_basis_path": str(report_basis_file),
        "readiness_status": readiness_status,
        "supervisor_status": supervisor_status,
        "report_basis_transition_request_id": report_basis_transition_request_id,
        "supervisor_transition_request_id": supervisor_transition_request_id,
        "supervisor_freshness_status": supervisor_freshness_status,
        "readiness_path": str(readiness_file),
        "board_brief_path": str(board_brief_file),
        "supervisor_state_path": str(supervisor_file),
        **contract_fields,
        "report_basis_id": maybe_text(report_basis_freeze.get("basis_id")),
        "basis_selection_mode": maybe_text(report_basis_freeze.get("basis_selection_mode")),
        "selected_basis_object_ids": selected_basis_object_ids,
        "supporting_proposal_ids": supporting_proposal_ids,
        "rejected_proposal_ids": rejected_proposal_ids,
        "supporting_opinion_ids": supporting_opinion_ids,
        "rejected_opinion_ids": rejected_opinion_ids,
        "report_basis_resolution_mode": maybe_text(
            report_basis_freeze.get("report_basis_resolution_mode")
        ),
        "report_basis_resolution_reasons": (
            report_basis_freeze.get("report_basis_resolution_reasons", [])
            if isinstance(report_basis_freeze.get("report_basis_resolution_reasons"), list)
            else []
        ),
        "council_input_counts": council_input_counts,
        "selected_evidence_refs": selected_evidence_refs,
        "evidence_packet": evidence_packet,
        "decision_packet": decision_packet,
        "report_packet": report_packet,
        "evidence_index": list_items(evidence_packet.get("evidence_index")),
        "uncertainty_register": list_items(report_packet.get("uncertainty_register")),
        "residual_disputes": list_items(report_packet.get("residual_disputes")),
        "policy_recommendations": list_items(report_packet.get("policy_recommendations")),
        "claim_gap_action_cards_path": maybe_text(
            claim_gap_cards_context.get("path")
        ),
        "claim_gap_action_card_count": len(claim_gap_action_cards),
        "claim_gap_action_cards": claim_gap_action_cards,
        "lane_episode_card_count": len(lane_episode_cards),
        "lane_episode_cards": lane_episode_cards,
        "interaction_timeline_path": maybe_text(
            interaction_timeline_context.get("path")
        ),
        "interaction_timeline_node_count": len(interaction_nodes),
        "interaction_timeline_nodes": interaction_nodes,
        "acquisition_checkpoint_path": maybe_text(
            acquisition_checkpoint_context.get("path")
        ),
        "acquisition_checkpoint_count": len(acquisition_checkpoints),
        "acquisition_checkpoints": acquisition_checkpoints,
        "theme_sufficiency_review_path": maybe_text(
            theme_sufficiency_context.get("path")
        ),
        "theme_sufficiency_review_count": len(theme_sufficiency_reviews),
        "theme_sufficiency_reviews": theme_sufficiency_reviews,
        "theme_progress_review_count": len(theme_progress_reviews),
        "theme_progress_reviews": theme_progress_reviews,
        "council_investigation_program_count": len(council_programs),
        "council_investigation_programs": council_programs,
        "section_brief_count": len(section_briefs),
        "agent_section_brief_count": len(agent_section_briefs),
        "derived_interaction_section_brief_count": len(interaction_section_briefs),
        "section_briefs": section_briefs,
        "challenger_constraint_count": len(
            list_items(report_basis_freeze.get("challenger_constraints"))
        )
        or len(list_items(readiness.get("challenger_constraints"))),
        "unresolved_challenger_constraint_count": len(
            unresolved_challenger_constraints
        ),
        "challenger_constraints": (
            list_items(report_basis_freeze.get("challenger_constraints"))
            or list_items(readiness.get("challenger_constraints"))
        ),
        "unresolved_challenger_constraints": unresolved_challenger_constraints,
        "basis_use_constraints": basis_use_constraints,
        "accepted_limitations": accepted_limitations,
        "unresolved_challenges": unresolved_challenges,
        "report_basis_input_policy": report_basis_input_policy(),
        "report_claim_structure": (
            report_basis_freeze.get("report_claim_structure", {})
            if isinstance(report_basis_freeze.get("report_claim_structure"), dict)
            else {}
        ),
        "explicit_report_claim_count": int(
            report_basis_freeze.get("explicit_report_claim_count") or 0
        ),
        "explicit_report_claim_objects": list_items(
            report_basis_freeze.get("explicit_report_claim_objects")
        ),
        "report_claim_structural_violation_count": len(
            report_claim_structural_violations
        ),
        "report_claim_structural_violations": report_claim_structural_violations,
        "explicit_lead_basis_count": int(
            report_basis_freeze.get("explicit_lead_basis_count") or 0
        ),
        "explicit_lead_basis_objects": list_items(
            report_basis_freeze.get("explicit_lead_basis_objects")
        ),
        "lead_basis_constraint_violation_count": len(
            lead_basis_constraint_violations
        ),
        "lead_basis_constraint_violations": lead_basis_constraint_violations,
        "board_brief_excerpt": board_excerpt,
        "key_findings": key_findings,
        "open_risks": open_risks,
        "recommended_next_actions": next_actions,
        "recommended_sections": recommended_sections(reporting_ready),
        "report_targets": ["expert-report-draft", "council-decision-draft"] if reporting_ready else ["expert-report-draft", "another-round-decision"],
        "warnings": warnings,
    }
    stored_payload = store_reporting_handoff_record(
        run_dir_path,
        handoff_payload=wrapper,
        artifact_path=str(output_file),
    )
    handoff_id = maybe_text(stored_payload.get("handoff_id")) or handoff_id
    write_json_file(output_file, stored_payload)

    artifact_refs = [{"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"}]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "basis_round_id": basis_round_id,
            "output_path": str(output_file),
            "handoff_id": handoff_id,
            "handoff_status": handoff_status,
            "reporting_ready": reporting_ready,
            "finding_count": len(key_findings),
            "evidence_packet_id": maybe_text(evidence_packet.get("packet_id")),
            "decision_packet_id": maybe_text(decision_packet.get("packet_id")),
            "report_packet_id": maybe_text(report_packet.get("packet_id")),
            "evidence_index_count": len(list_items(evidence_packet.get("evidence_index"))),
            "acquisition_checkpoint_count": len(acquisition_checkpoints),
            "theme_sufficiency_review_count": len(theme_sufficiency_reviews),
            "theme_progress_review_count": len(theme_progress_reviews),
            "council_investigation_program_count": len(council_programs),
            "agent_section_brief_count": len(agent_section_briefs),
            "derived_interaction_section_brief_count": len(interaction_section_briefs),
            "unresolved_challenger_constraint_count": len(
                unresolved_challenger_constraints
            ),
            "lead_basis_constraint_violation_count": len(
                lead_basis_constraint_violations
            ),
            "report_claim_structural_violation_count": len(
                report_claim_structural_violations
            ),
            "board_state_source": contract_fields["board_state_source"],
            "coverage_source": contract_fields["coverage_source"],
            "report_basis_source": maybe_text(contract_fields.get("report_basis_source")),
            "readiness_source": maybe_text(contract_fields.get("readiness_source")),
            "supervisor_state_source": maybe_text(contract_fields.get("supervisor_state_source")),
            "db_path": contract_fields["db_path"],
        },
        "receipt_id": "reporting-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, handoff_id)[:20],
        "batch_id": "reportingbatch-" + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [handoff_id],
        "warnings": warnings,
        "deliberation_sync": contract_fields["deliberation_sync"],
        "analysis_sync": contract_fields["analysis_sync"],
        "board_handoff": {
            "candidate_ids": [handoff_id],
            "evidence_refs": artifact_refs,
            "gap_hints": unique_texts(
                [item.get("summary", "") for item in open_risks[:3] if maybe_text(item.get("summary"))]
                + (reporting_blocker_hints if not reporting_ready else [])
            )[:3] if not reporting_ready else [],
            "challenge_hints": [item.get("summary", "") for item in open_risks[:2] if maybe_text(item.get("summary"))],
            "suggested_next_skills": ["draft-expert-report", "draft-council-decision"] if reporting_ready else ["draft-expert-report", "draft-council-decision", "submit-finding-record", "submit-evidence-bundle", "submit-council-proposal", "submit-readiness-opinion"],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize a compact DB-backed reporting handoff from frozen evidence basis.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--report-basis-path", default="")
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
        report_basis_path=args.report_basis_path,
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
