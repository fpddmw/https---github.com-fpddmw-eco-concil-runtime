#!/usr/bin/env python3
"""Summarize round-level readiness from board, D1, and evidence artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any

SKILL_NAME = "eco-summarize-round-readiness"
WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
RUNTIME_SRC = WORKSPACE_ROOT / "eco-concil-runtime" / "src"
if str(RUNTIME_SRC) not in sys.path:
    sys.path.insert(0, str(RUNTIME_SRC))

from eco_council_runtime.council_objects import (  # noqa: E402
    query_council_objects,
)
from eco_council_runtime.phase2_fallback_common import (  # noqa: E402
    maybe_text,
    resolve_path,
)
from eco_council_runtime.phase2_fallback_context import (  # noqa: E402
    load_d1_shared_context,
)
from eco_council_runtime.phase2_fallback_contracts import (  # noqa: E402
    d1_contract_fields_from_payload,
)
from eco_council_runtime.kernel.deliberation_plane import (  # noqa: E402
    store_round_readiness_assessment,
)
from eco_council_runtime.kernel.phase2_state_surfaces import (  # noqa: E402
    load_falsification_probe_wrapper,
    load_next_actions_wrapper,
)


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


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def list_items(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


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


def summarize_counts(items: list[dict[str, Any]], *, field_name: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        value = maybe_text(item.get(field_name))
        if not value:
            continue
        counts[value] = counts.get(value, 0) + 1
    return counts


def collected_evidence_refs(items: list[dict[str, Any]]) -> list[str]:
    values: list[Any] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        values.extend(list_items(item.get("evidence_refs")))
    return unique_texts(values)


def load_council_readiness_opinions(
    run_dir: Path,
    *,
    run_id: str,
    round_id: str,
) -> list[dict[str, Any]]:
    payload = query_council_objects(
        run_dir,
        object_kind="readiness-opinion",
        run_id=run_id,
        round_id=round_id,
        limit=200,
    )
    opinions = (
        payload.get("objects", [])
        if isinstance(payload.get("objects"), list)
        else []
    )
    return [
        opinion
        for opinion in opinions
        if isinstance(opinion, dict)
        and maybe_text(opinion.get("opinion_status")) not in {"withdrawn", "retracted"}
    ]


def readiness_bucket(opinion: dict[str, Any]) -> str:
    readiness_value = maybe_text(opinion.get("readiness_status"))
    if bool(opinion.get("sufficient_for_promotion")) or readiness_value in {
        "ready",
        "ready-for-promotion",
        "promote",
    }:
        return "ready"
    if readiness_value in {"blocked", "reject", "rejected"}:
        return "blocked"
    return "needs-more-data"


def aggregate_council_readiness_opinions(
    opinions: list[dict[str, Any]],
) -> dict[str, Any]:
    ready_opinions = [
        opinion for opinion in opinions if readiness_bucket(opinion) == "ready"
    ]
    blocked_opinions = [
        opinion for opinion in opinions if readiness_bucket(opinion) == "blocked"
    ]
    needs_more_data_opinions = [
        opinion
        for opinion in opinions
        if readiness_bucket(opinion) == "needs-more-data"
    ]
    if ready_opinions and not blocked_opinions and not needs_more_data_opinions:
        readiness_value = "ready"
        lead_reason = (
            f"Council submitted {len(ready_opinions)} readiness opinions and all support promotion."
        )
    elif blocked_opinions and not ready_opinions:
        readiness_value = "blocked"
        lead_reason = (
            f"Council submitted {len(opinions)} readiness opinions and none support promotion."
        )
    else:
        readiness_value = "needs-more-data"
        lead_reason = (
            f"Council readiness opinions are mixed across {len(opinions)} submitted opinions, so the round stays open."
        )
    detailed_reasons = [
        f"{maybe_text(opinion.get('agent_role')) or 'agent'}: {maybe_text(opinion.get('rationale'))}"
        for opinion in opinions
        if maybe_text(opinion.get("rationale"))
    ]
    opinion_ids = unique_texts(
        [opinion.get("opinion_id") for opinion in opinions]
    )
    basis_object_ids = unique_texts(
        [
            basis_object_id
            for opinion in opinions
            for basis_object_id in list_items(opinion.get("basis_object_ids"))
        ]
    )
    evidence_refs = unique_texts(
        [
            evidence_ref
            for opinion in opinions
            for evidence_ref in list_items(opinion.get("evidence_refs"))
        ]
    )
    return {
        "readiness_status": readiness_value,
        "reasons": [lead_reason, *detailed_reasons[:3]],
        "opinion_ids": opinion_ids,
        "basis_object_ids": basis_object_ids,
        "evidence_refs": evidence_refs,
        "opinion_status_counts": {
            "ready": len(ready_opinions),
            "blocked": len(blocked_opinions),
            "needs-more-data": len(needs_more_data_opinions),
        },
    }


def readiness_status(
    *,
    active_hypotheses: int,
    issue_cluster_count: int,
    empirical_issue_count: int,
    strong_coverages: int,
    moderate_coverages: int,
    open_challenges: int,
    open_tasks: int,
    open_probes: int,
    high_priority_actions: int,
    routing_actions: int,
    empirical_gap_actions: int,
    representation_gap_actions: int,
    formal_linkage_actions: int,
    issue_gap_actions: int,
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if active_hypotheses == 0 and issue_cluster_count == 0:
        reasons.append("No active board hypotheses or controversy-map issues are available for round-level review.")
        return "blocked", reasons
    if empirical_issue_count > 0 and strong_coverages + moderate_coverages == 0:
        reasons.append("Empirical controversy issues are present but no moderate-or-strong evidence coverage objects are available.")
        return "blocked", reasons
    if open_challenges > 0:
        reasons.append(f"{open_challenges} contested points remain open.")
    if open_tasks > 0:
        reasons.append(f"{open_tasks} board coordination tasks remain in flight.")
    if open_probes > 0:
        reasons.append(f"{open_probes} controversy probes remain open.")
    if routing_actions > 0:
        reasons.append(f"{routing_actions} verification-routing actions remain unresolved.")
    if empirical_gap_actions > 0:
        reasons.append(f"{empirical_gap_actions} empirical verification or contradiction-resolution actions remain unresolved.")
    if representation_gap_actions > 0:
        reasons.append(f"{representation_gap_actions} representation-gap actions remain unresolved.")
    if formal_linkage_actions > 0 and representation_gap_actions == 0:
        reasons.append(f"{formal_linkage_actions} formal/public linkage actions remain unresolved.")
    if issue_gap_actions > 0:
        reasons.append(f"{issue_gap_actions} issue-structure or contestation actions remain unresolved.")
    if high_priority_actions > 0 and not reasons:
        reasons.append(f"{high_priority_actions} high-priority investigation actions remain unresolved.")
    if reasons:
        return "needs-more-data", reasons
    if strong_coverages > 0:
        reasons.append("At least one strong evidence coverage object is available and no controversy-routing blockers remain.")
    elif moderate_coverages > 0 and empirical_issue_count == 0:
        reasons.append("No empirical blockers remain and the current non-empirical issue set is coherent enough for promotion review.")
    elif moderate_coverages > 0:
        reasons.append("Empirical issues are covered at least moderately and no remaining blockers are visible.")
    else:
        reasons.append("No empirical blockers remain and the current issue map is coherent enough for promotion review.")
    return "ready", reasons


def summarize_round_readiness_skill(
    run_dir: str,
    run_id: str,
    round_id: str,
    board_summary_path: str,
    board_brief_path: str,
    next_actions_path: str,
    probes_path: str,
    coverage_path: str,
    output_path: str,
) -> dict[str, Any]:
    run_dir_path = resolve_run_dir(run_dir)
    board_summary_file = resolve_path(run_dir_path, board_summary_path, f"board/board_state_summary_{round_id}.json")
    board_brief_file = resolve_path(run_dir_path, board_brief_path, f"board/board_brief_{round_id}.md")
    next_actions_file = resolve_path(run_dir_path, next_actions_path, f"investigation/next_actions_{round_id}.json")
    probes_file = resolve_path(run_dir_path, probes_path, f"investigation/falsification_probes_{round_id}.json")
    output_file = resolve_path(run_dir_path, output_path, f"reporting/round_readiness_{round_id}.json")

    shared_context = load_d1_shared_context(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        board_summary_path=board_summary_path,
        board_brief_path=board_brief_path,
        coverage_path=coverage_path,
        include_board_notes=True,
    )
    warnings = (
        shared_context.get("warnings", [])
        if isinstance(shared_context.get("warnings"), list)
        else []
    )
    next_actions_context = load_next_actions_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        next_actions_path=next_actions_path,
    )
    next_actions_payload = (
        next_actions_context.get("payload")
        if isinstance(next_actions_context.get("payload"), dict)
        else None
    )
    next_actions_artifact_present = bool(next_actions_context.get("artifact_present"))
    next_actions_present = bool(next_actions_context.get("payload_present"))
    next_actions = next_actions_payload if isinstance(next_actions_payload, dict) else {"ranked_actions": [], "action_count": 0}
    probes_context = load_falsification_probe_wrapper(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
        probes_path=probes_path,
    )
    probes_payload = (
        probes_context.get("payload")
        if isinstance(probes_context.get("payload"), dict)
        else None
    )
    probes_artifact_present = bool(probes_context.get("artifact_present"))
    probes_present = bool(probes_context.get("payload_present"))
    probes = probes_payload if isinstance(probes_payload, dict) else {"probes": [], "probe_count": 0}
    council_opinions = load_council_readiness_opinions(
        run_dir_path,
        run_id=run_id,
        round_id=round_id,
    )
    contract_fields = d1_contract_fields_from_payload(
        shared_context,
        observed_inputs_overrides={
            "next_actions_artifact_present": next_actions_artifact_present,
            "next_actions_present": next_actions_present,
            "probes_artifact_present": probes_artifact_present,
            "probes_present": probes_present,
        },
    )
    coverages = (
        shared_context.get("coverages", [])
        if isinstance(shared_context.get("coverages"), list)
        else []
    )
    coverage_file = maybe_text(shared_context.get("coverage_file"))
    brief_excerpt = maybe_text(shared_context.get("board_brief_text"))[:220]
    board_state = (
        shared_context.get("board_state")
        if isinstance(shared_context.get("board_state"), dict)
        else {}
    )
    agenda_counts = (
        shared_context.get("agenda_counts")
        if isinstance(shared_context.get("agenda_counts"), dict)
        else {}
    )

    strong_coverages = len([item for item in coverages if maybe_text(item.get("readiness")) == "strong"])
    moderate_coverages = len([item for item in coverages if maybe_text(item.get("readiness")) == "moderate"])
    weak_coverages = len([item for item in coverages if maybe_text(item.get("readiness")) == "weak"])

    counts = board_state.get("counts", {}) if isinstance(board_state.get("counts"), dict) else {}
    active_hypotheses = int(counts.get("hypotheses_active") or len(board_state.get("active_hypotheses", [])))
    open_challenges = int(counts.get("challenge_open") or len(board_state.get("open_challenges", [])))
    open_tasks = int(counts.get("tasks_open") or len(board_state.get("open_tasks", [])))
    issue_cluster_count = int(agenda_counts.get("issue_cluster_count") or 0)
    empirical_issue_count = int(agenda_counts.get("empirical_issue_count") or 0)
    non_empirical_issue_count = int(agenda_counts.get("non_empirical_issue_count") or 0)
    mixed_issue_count = int(agenda_counts.get("mixed_issue_count") or 0)
    open_probes = len([item for item in probes.get("probes", []) if isinstance(item, dict) and maybe_text(item.get("probe_status")) not in {"closed", "cancelled"}]) if isinstance(probes.get("probes"), list) else 0
    high_priority_actions = len(
        [
            item
            for item in next_actions.get("ranked_actions", [])
            if isinstance(item, dict)
            and maybe_text(item.get("priority")) in {"high", "critical"}
            and maybe_text(item.get("action_kind")) != "prepare-promotion"
        ]
    ) if isinstance(next_actions.get("ranked_actions"), list) else 0
    action_gap_counts = summarize_counts(
        next_actions.get("ranked_actions", [])
        if isinstance(next_actions.get("ranked_actions"), list)
        else [],
        field_name="controversy_gap",
    )
    probe_type_counts = summarize_counts(
        probes.get("probes", []) if isinstance(probes.get("probes"), list) else [],
        field_name="probe_type",
    )
    routing_actions = max(
        int(action_gap_counts.get("verification-routing-gap", 0)),
        int(agenda_counts.get("routing_issue_count") or 0),
    )
    empirical_gap_actions = max(
        int(action_gap_counts.get("verification-gap", 0))
        + int(action_gap_counts.get("formal-public-misalignment", 0)),
        int(agenda_counts.get("empirical_issue_gap_count") or 0),
    )
    representation_gap_actions = max(
        int(action_gap_counts.get("representation-gap", 0)),
        int(agenda_counts.get("representation_gap_count") or 0),
    )
    formal_linkage_actions = max(
        int(action_gap_counts.get("formal-record-gap", 0))
        + int(action_gap_counts.get("formal-public-linkage-gap", 0))
        + int(action_gap_counts.get("public-discourse-gap", 0))
        + int(action_gap_counts.get("stakeholder-deliberation-gap", 0)),
        int(agenda_counts.get("formal_public_linkage_gap_count") or 0),
    )
    issue_gap_actions = int(action_gap_counts.get("issue-structure-gap", 0)) + int(
        action_gap_counts.get("unresolved-contestation", 0)
    )
    diffusion_focus_count = int(agenda_counts.get("diffusion_focus_count") or 0)

    status_value, reasons = readiness_status(
        active_hypotheses=active_hypotheses,
        issue_cluster_count=issue_cluster_count,
        empirical_issue_count=empirical_issue_count,
        strong_coverages=strong_coverages,
        moderate_coverages=moderate_coverages,
        open_challenges=open_challenges,
        open_tasks=open_tasks,
        open_probes=open_probes,
        high_priority_actions=high_priority_actions,
        routing_actions=routing_actions,
        empirical_gap_actions=empirical_gap_actions,
        representation_gap_actions=representation_gap_actions,
        formal_linkage_actions=formal_linkage_actions,
        issue_gap_actions=issue_gap_actions,
    )
    decision_source = "policy-fallback"
    readiness_lineage = unique_texts(
        [
            item.get("action_id")
            for item in next_actions.get("ranked_actions", [])
            if isinstance(item, dict)
        ]
        + [
            item.get("probe_id")
            for item in probes.get("probes", [])
            if isinstance(item, dict)
        ]
    )
    readiness_evidence_refs = unique_texts(
        collected_evidence_refs(coverages)
        + collected_evidence_refs(
            next_actions.get("ranked_actions", [])
            if isinstance(next_actions.get("ranked_actions"), list)
            else []
        )
        + collected_evidence_refs(
            probes.get("probes", [])
            if isinstance(probes.get("probes"), list)
            else []
        )
    )
    selected_basis_object_ids: list[str] = []
    opinion_ids: list[str] = []
    opinion_status_counts: dict[str, int] = {}
    if council_opinions:
        council_summary = aggregate_council_readiness_opinions(council_opinions)
        status_value = maybe_text(council_summary.get("readiness_status")) or status_value
        reasons = (
            council_summary.get("reasons", [])
            if isinstance(council_summary.get("reasons"), list)
            else reasons
        )
        decision_source = "agent-council"
        selected_basis_object_ids = list_items(
            council_summary.get("basis_object_ids")
        )
        opinion_ids = list_items(council_summary.get("opinion_ids"))
        opinion_status_counts = (
            council_summary.get("opinion_status_counts", {})
            if isinstance(council_summary.get("opinion_status_counts"), dict)
            else {}
        )
        readiness_evidence_refs = unique_texts(
            list_items(council_summary.get("evidence_refs"))
            + readiness_evidence_refs
        )
        readiness_lineage = unique_texts(opinion_ids + selected_basis_object_ids)
    findings = [
        {"finding_id": "readiness-coverage", "title": "Coverage posture", "summary": f"strong={strong_coverages}, moderate={moderate_coverages}, weak={weak_coverages}", "confidence": "medium"},
        {"finding_id": "readiness-board", "title": "Board posture", "summary": f"active_hypotheses={active_hypotheses}, open_challenges={open_challenges}, open_tasks={open_tasks}", "confidence": "medium"},
        {
            "finding_id": "readiness-investigation",
            "title": "Investigation posture",
            "summary": f"open_probes={open_probes}, high_priority_actions={high_priority_actions}, routing_actions={routing_actions}, empirical_gap_actions={empirical_gap_actions}, representation_gap_actions={representation_gap_actions}",
            "confidence": "medium",
        },
        {
            "finding_id": "readiness-controversy-map",
            "title": "Controversy map posture",
            "summary": f"issue_clusters={issue_cluster_count}, empirical_issues={empirical_issue_count}, non_empirical_issues={non_empirical_issue_count}, mixed_issues={mixed_issue_count}, formal_linkage_actions={formal_linkage_actions}, diffusion_focus_count={diffusion_focus_count}, action_gaps={json.dumps(action_gap_counts, ensure_ascii=True, sort_keys=True)}",
            "confidence": "medium",
        },
    ]
    if brief_excerpt:
        findings.append({"finding_id": "readiness-brief", "title": "Board brief context", "summary": brief_excerpt, "confidence": "low"})

    if status_value == "ready":
        recommended_next_skills = ["eco-promote-evidence-basis"]
    else:
        recommended_next_skills = ["eco-propose-next-actions", "eco-post-board-note"]
        if open_probes > 0 or routing_actions > 0 or empirical_gap_actions > 0:
            recommended_next_skills.append("eco-open-falsification-probe")
        if representation_gap_actions > 0 or formal_linkage_actions > 0:
            recommended_next_skills.append("eco-link-formal-comments-to-public-discourse")
            recommended_next_skills.append("eco-identify-representation-gaps")
        if diffusion_focus_count > 0:
            recommended_next_skills.append("eco-detect-cross-platform-diffusion")
        deduped: list[str] = []
        for skill_name in recommended_next_skills:
            if skill_name not in deduped:
                deduped.append(skill_name)
        recommended_next_skills = deduped
    wrapper = {
        "schema_version": "d2.1",
        "skill": SKILL_NAME,
        "generated_at_utc": utc_now_iso(),
        "run_id": run_id,
        "round_id": round_id,
        "board_summary_path": str(board_summary_file),
        "board_brief_path": str(board_brief_file),
        "next_actions_path": str(next_actions_file),
        "probes_path": str(probes_file),
        "coverage_path": str(coverage_file),
        **contract_fields,
        "next_actions_source": maybe_text(next_actions_context.get("source"))
        or "missing-next-actions",
        "probes_source": maybe_text(probes_context.get("source"))
        or "missing-probes",
        "decision_source": decision_source,
        "readiness_status": status_value,
        "sufficient_for_promotion": status_value == "ready",
        "evidence_refs": readiness_evidence_refs,
        "lineage": readiness_lineage,
        "provenance": {
            "source_skill": SKILL_NAME,
            "decision_source": decision_source,
            "board_state_source": contract_fields["board_state_source"],
            "coverage_source": contract_fields["coverage_source"],
            "next_actions_source": maybe_text(next_actions_context.get("source"))
            or "missing-next-actions",
            "probes_source": maybe_text(probes_context.get("source"))
            or "missing-probes",
            "council_opinion_count": len(council_opinions),
        },
        "opinion_ids": opinion_ids,
        "selected_basis_object_ids": selected_basis_object_ids,
        "readiness_opinion_count": len(council_opinions),
        "readiness_opinion_status_counts": opinion_status_counts,
        "agenda_counts": agenda_counts,
        "counts": {
            "active_hypotheses": active_hypotheses,
            "issue_clusters": issue_cluster_count,
            "empirical_issues": empirical_issue_count,
            "non_empirical_issues": non_empirical_issue_count,
            "mixed_issues": mixed_issue_count,
            "open_challenges": open_challenges,
            "open_tasks": open_tasks,
            "open_probes": open_probes,
            "strong_coverages": strong_coverages,
            "moderate_coverages": moderate_coverages,
            "weak_coverages": weak_coverages,
            "high_priority_actions": high_priority_actions,
            "routing_actions": routing_actions,
            "empirical_gap_actions": empirical_gap_actions,
            "representation_gap_actions": representation_gap_actions,
            "formal_linkage_actions": formal_linkage_actions,
            "issue_gap_actions": issue_gap_actions,
            "diffusion_focus_count": diffusion_focus_count,
            "agent_readiness_opinions": len(council_opinions),
        },
        "controversy_gap_counts": action_gap_counts,
        "probe_type_counts": probe_type_counts,
        "gate_reasons": reasons,
        "findings": findings[:4],
        "recommended_next_skills": recommended_next_skills,
    }
    wrapper = store_round_readiness_assessment(
        run_dir_path,
        readiness_payload=wrapper,
        artifact_path=str(output_file),
    )
    write_json_file(output_file, wrapper)
    readiness_id = maybe_text(wrapper.get("readiness_id")) or (
        "round-readiness-" + stable_hash(run_id, round_id, status_value)[:12]
    )
    artifact_refs = [{"signal_id": "", "artifact_path": str(output_file), "record_locator": "$", "artifact_ref": f"{output_file}:$"}]
    return {
        "status": "completed",
        "summary": {
            "skill": SKILL_NAME,
            "run_id": run_id,
            "round_id": round_id,
            "output_path": str(output_file),
            "readiness_status": status_value,
            "readiness_id": readiness_id,
            "board_state_source": contract_fields["board_state_source"],
            "coverage_source": contract_fields["coverage_source"],
            "db_path": contract_fields["db_path"],
        },
        "receipt_id": "reporting-receipt-" + stable_hash(SKILL_NAME, run_id, round_id, readiness_id)[:20],
        "batch_id": "reportingbatch-" + stable_hash(SKILL_NAME, run_id, round_id, output_file.name)[:16],
        "artifact_refs": artifact_refs,
        "canonical_ids": [readiness_id],
        "warnings": warnings,
        "deliberation_sync": contract_fields["deliberation_sync"],
        "analysis_sync": contract_fields["analysis_sync"],
        "board_handoff": {
            "candidate_ids": [readiness_id],
            "evidence_refs": artifact_refs,
            "gap_hints": [] if status_value == "ready" else reasons[:3],
            "challenge_hints": [reason for reason in reasons if "challenge" in reason.lower() or "probe" in reason.lower()],
            "suggested_next_skills": recommended_next_skills,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize round-level readiness from board, D1, and evidence artifacts.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--round-id", required=True)
    parser.add_argument("--board-summary-path", default="")
    parser.add_argument("--board-brief-path", default="")
    parser.add_argument("--next-actions-path", default="")
    parser.add_argument("--probes-path", default="")
    parser.add_argument("--coverage-path", default="")
    parser.add_argument("--output-path", default="")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = summarize_round_readiness_skill(
        run_dir=args.run_dir,
        run_id=args.run_id,
        round_id=args.round_id,
        board_summary_path=args.board_summary_path,
        board_brief_path=args.board_brief_path,
        next_actions_path=args.next_actions_path,
        probes_path=args.probes_path,
        coverage_path=args.coverage_path,
        output_path=args.output_path,
    )
    print(pretty_json(payload, args.pretty))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
