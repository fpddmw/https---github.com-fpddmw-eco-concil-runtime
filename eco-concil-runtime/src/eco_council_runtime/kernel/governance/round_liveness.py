from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from eco_council_runtime.kernel.governance.claim_strength import (
    claim_strength_closing_item,
    claim_strength_obligations,
)
from eco_council_runtime.kernel.governance.evidence_route_assessment import (
    COMPACT_LIST_FIELDS as ROUTE_ASSESSMENT_COMPACT_LIST_FIELDS,
    COMPACT_TEXT_FIELDS as ROUTE_ASSESSMENT_COMPACT_TEXT_FIELDS,
    route_assessment_closing_item,
    route_assessment_needs_moderator_response,
)
from eco_council_runtime.objects.council import query_council_objects
from eco_council_runtime.runtime_command_hints import kernel_command, run_skill_command


TERMINAL_STATUSES = {
    "closed",
    "completed",
    "done",
    "executed",
    "normalized",
    "rejected",
    "resolved",
    "retired",
    "withdrawn",
}
READY_STATUSES = {"ready", "report-ready"}
NONPRODUCTIVE_SOURCE_ATTEMPT_STATUSES = {"blocked", "failed", "receipt-only"}


def maybe_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


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


def text_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return unique_texts(value)


def object_id_for_kind(object_kind: str, payload: dict[str, Any]) -> str:
    for field_name in (
        "object_id",
        "finding_id",
        "bundle_id",
        "hypothesis_id",
        "ticket_id",
        "task_id",
        "opinion_id",
        "request_id",
        "proposal_id",
        "assessment_id",
        "synthesis_id",
    ):
        text = maybe_text(payload.get(field_name))
        if text:
            return text
    return ""


def object_ref(object_kind: str, payload: dict[str, Any]) -> str:
    identifier = object_id_for_kind(object_kind, payload)
    return f"{object_kind}:{identifier}" if identifier else ""


def object_agent_role(payload: dict[str, Any], *, fallback: str = "<agent_role>") -> str:
    return (
        maybe_text(payload.get("agent_role"))
        or maybe_text(payload.get("author_role"))
        or maybe_text(payload.get("owner_role"))
        or fallback
    )


def continuation_payload_for_ref(object_ref_value: str) -> dict[str, Any]:
    return {
        "round_mode": "continuation",
        "primary_focus_refs": [object_ref_value],
        "continuation_basis": "moderator-selected unresolved ref",
        "closure_reason_if_not_continuing": (
            "<report-ready|no-actionable-path|human-paused|out-of-scope>"
        ),
    }


def carry_to_next_round_command_template(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    next_round_id: str,
    object_ref_value: str,
) -> str:
    return kernel_command(
        "request-phase-transition",
        "--run-dir",
        str(run_dir),
        "--run-id",
        run_id,
        "--round-id",
        round_id,
        "--transition-kind",
        "open-investigation-round",
        "--target-round-id",
        maybe_text(next_round_id) or "<target_round_id>",
        "--source-round-id",
        round_id,
        "--request-payload-json",
        json.dumps(
            continuation_payload_for_ref(object_ref_value),
            ensure_ascii=True,
            sort_keys=True,
        ),
        "--rationale",
        "<moderator_continuation_rationale>",
        actor_role="moderator",
    )


def object_handoff_commands(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    next_round_id: str,
    object_kind: str,
    payload: dict[str, Any],
) -> dict[str, str]:
    identifier = object_id_for_kind(object_kind, payload)
    object_ref_value = f"{object_kind}:{identifier}" if identifier else ""
    if not object_ref_value:
        return {}

    handoff_commands: dict[str, str] = {
        "carry_to_next_round_command_template": carry_to_next_round_command_template(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            next_round_id=next_round_id,
            object_ref_value=object_ref_value,
        )
    }
    agent_role = object_agent_role(payload)

    if object_kind == "finding":
        handoff_commands[
            "submit_evidence_bundle_from_finding_command_template"
        ] = kernel_command(
            "submit-evidence-bundle",
            "--run-dir",
            str(run_dir),
            "--run-id",
            run_id,
            "--round-id",
            round_id,
            "--actor-role",
            agent_role,
            "--agent-role",
            agent_role,
            "--bundle-kind",
            "evidence-bundle",
            "--title",
            "<bundle_title>",
            "--summary",
            "<bundle_summary>",
            "--rationale",
            "<agent_bundle_rationale>",
            "--target-kind",
            "finding",
            "--target-id",
            identifier,
            "--basis-object-id",
            identifier,
            "--finding-id",
            identifier,
            "--evidence-ref",
            "<finding_evidence_ref>",
            "--provenance-json",
            "{\"source\":\"agent-follow-up\"}",
        )
        handoff_commands[
            "update_hypothesis_from_finding_command_template"
        ] = run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="update-hypothesis-status",
            actor_role=agent_role,
            contract_mode="warn",
            skill_args=[
                "--title",
                "<provisional_hypothesis_title>",
                "--statement",
                "<hypothesis_statement>",
                "--status",
                "active",
                "--owner-role",
                agent_role,
                "--linked-artifact-ref",
                f"finding:{identifier}",
                "--evidence-ref",
                "<finding_evidence_ref>",
            ],
        )
    elif object_kind == "evidence-request":
        handoff_commands[
            "submit_source_acquisition_proposal_for_request_command_template"
        ] = run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="submit-source-acquisition-proposal",
            actor_role="<agent_role>",
            contract_mode="warn",
            skill_args=[
                "--author-role",
                "<agent_role>",
                "--source-skill",
                "<source_skill>",
                "--query-parameters-json",
                "<query_parameters_json>",
                "--target-kind",
                "evidence-request",
                "--target-id",
                identifier,
                "--target-evidence-request-id",
                identifier,
                "--rationale",
                "<agent_source_acquisition_rationale>",
                "--provenance-json",
                "{\"source\":\"agent-follow-up\"}",
            ],
        )
        handoff_commands[
            "submit_evidence_route_assessment_for_request_command_template"
        ] = run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="submit-evidence-route-assessment",
            actor_role="<agent_role>",
            contract_mode="warn",
            skill_args=[
                "--author-role",
                "<agent_role>",
                "--assessment-type",
                "<source-surface-mismatch|capability-gap|route-discovery-needed|no-actionable-current-route|same-family-followup-needed>",
                "--evidence-need-summary",
                "<what_this_request_needs>",
                "--current-surface-summary",
                "<what_visible_sources_or_skills_can_and_cannot_answer>",
                "--route-judgment",
                "<route_judgment>",
                "--recommended-next-step",
                "<route-discovery-continuation|capability-gap-human-pause|bounded-report-with-limitation|revise-request>",
                "--target-kind",
                "evidence-request",
                "--target-id",
                identifier,
                "--target-evidence-request-id",
                identifier,
                "--rationale",
                "<agent_route_assessment_rationale>",
                "--provenance-json",
                "{\"source\":\"agent-follow-up\"}",
            ],
        )
    elif object_kind == "source-acquisition-proposal":
        handoff_commands[
            "update_source_acquisition_proposal_status_command_template"
        ] = run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="update-source-acquisition-proposal-status",
            actor_role="<actor_role>",
            contract_mode="warn",
            skill_args=[
                "--object-id",
                identifier,
                "--status",
                "<proposed|approved-for-execution|fetched|normalized|receipt-only|failed|blocked|withdrawn|rejected>",
                "--actor-role",
                "<actor_role>",
                "--status-rationale",
                "<status_rationale>",
                "--evidence-ref",
                "<evidence_ref>",
                "--provenance-json",
                "{\"source\":\"agent-follow-up\"}",
            ],
        )
        handoff_commands[
            "link_source_acquisition_execution_command_template"
        ] = run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="link-source-acquisition-execution",
            actor_role="<actor_role>",
            contract_mode="warn",
            skill_args=[
                "--object-id",
                identifier,
                "--actor-role",
                "<actor_role>",
                "--status-rationale",
                "<execution_lineage_rationale>",
                "--fetch-receipt-ref",
                "<fetch_receipt_ref>",
                "--normalization-receipt-ref",
                "<normalization_receipt_ref>",
                "--normalized-signal-ref",
                "<normalized_signal_ref>",
                "--provenance-json",
                "{\"source\":\"agent-follow-up\"}",
            ],
        )
    elif object_kind == "hypothesis":
        handoff_commands[
            "open_challenge_on_hypothesis_command_template"
        ] = run_skill_command(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            skill_name="open-challenge-ticket",
            actor_role="challenger",
            contract_mode="warn",
            skill_args=[
                "--title",
                "<challenge_title>",
                "--challenge-statement",
                "<challenge_statement>",
                "--target-hypothesis-id",
                identifier,
                "--owner-role",
                "challenger",
                "--linked-artifact-ref",
                f"hypothesis:{identifier}",
                "--required-followup-evidence",
                "<followup_evidence_need>",
            ],
        )

    return handoff_commands


def compact_object(
    object_kind: str,
    payload: dict[str, Any],
    *,
    run_dir: Path | None = None,
    run_id: str = "",
    round_id: str = "",
    next_round_id: str = "<target_round_id>",
) -> dict[str, Any]:
    identifier = object_id_for_kind(object_kind, payload)
    compact: dict[str, Any] = {
        "object_kind": object_kind,
        "object_id": identifier,
        "object_ref": f"{object_kind}:{identifier}" if identifier else "",
        "status": maybe_text(payload.get("status"))
        or maybe_text(payload.get("readiness_status"))
        or maybe_text(payload.get("probe_status")),
        "agent_role": maybe_text(payload.get("agent_role"))
        or maybe_text(payload.get("author_role"))
        or maybe_text(payload.get("owner_role")),
        "target_kind": maybe_text(payload.get("target_kind")),
        "target_id": maybe_text(payload.get("target_id")),
        "title": maybe_text(payload.get("title")),
        "summary": maybe_text(payload.get("summary")),
        "rationale": maybe_text(payload.get("rationale")),
    }
    for field_name in (
        "question",
        "desired_evidence_type",
        "source_skill",
        "target_evidence_request_id",
        "readiness_status",
        "sufficient_for_report_basis",
        *ROUTE_ASSESSMENT_COMPACT_TEXT_FIELDS,
    ):
        value = payload.get(field_name)
        if isinstance(value, bool) or maybe_text(value):
            compact[field_name] = value
    for field_name in ROUTE_ASSESSMENT_COMPACT_LIST_FIELDS:
        values = text_list(payload.get(field_name))
        if values:
            compact[field_name] = values[:10]
            compact[field_name + "_count"] = len(values)
    evidence_refs = text_list(payload.get("evidence_refs"))
    if evidence_refs:
        compact["evidence_refs"] = evidence_refs[:10]
        compact["evidence_ref_count"] = len(evidence_refs)
    if run_dir is not None and run_id and round_id:
        handoff_commands = object_handoff_commands(
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            next_round_id=next_round_id,
            object_kind=object_kind,
            payload=payload,
        )
        if handoff_commands:
            compact["handoff_commands"] = handoff_commands
    return {
        key: value
        for key, value in compact.items()
        if value not in ("", [], {})
    }


def query_objects(
    run_dir: Path,
    *,
    object_kind: str,
    run_id: str,
    round_id: str,
    limit: int,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    try:
        payload = query_council_objects(
            run_dir,
            object_kind=object_kind,
            run_id=run_id,
            round_id=round_id,
            limit=limit,
        )
    except Exception as exc:  # noqa: BLE001
        return [], [
            {
                "code": "round-liveness-query-failed",
                "message": f"Could not query {object_kind}: {exc}",
            }
        ]
    return (
        [item for item in payload.get("objects", []) if isinstance(item, dict)]
        if isinstance(payload.get("objects"), list)
        else [],
        [],
    )


def is_open_status(value: Any) -> bool:
    status = maybe_text(value)
    return not status or status not in TERMINAL_STATUSES


def is_open_source_proposal(payload: dict[str, Any]) -> bool:
    status = maybe_text(payload.get("status"))
    if status == "executed":
        return True
    return is_open_status(status)


def is_not_ready_opinion(payload: dict[str, Any]) -> bool:
    readiness_status = maybe_text(payload.get("readiness_status"))
    if readiness_status in READY_STATUSES:
        return False
    if bool(payload.get("sufficient_for_report_basis")):
        return False
    return True


def is_nonproductive_source_attempt(payload: dict[str, Any]) -> bool:
    status = maybe_text(payload.get("status"))
    if status in NONPRODUCTIVE_SOURCE_ATTEMPT_STATUSES:
        return True
    if status == "executed" and not text_list(payload.get("normalized_signal_refs")):
        return True
    return False


def bundled_finding_ids(bundles: list[dict[str, Any]]) -> set[str]:
    ids: set[str] = set()
    for bundle in bundles:
        for field_name in ("finding_ids", "lineage", "basis_object_ids"):
            for value in text_list(bundle.get(field_name)):
                text = maybe_text(value)
                if text.startswith("finding:"):
                    text = text.split(":", 1)[1]
                if text.startswith("finding-"):
                    ids.add(text)
    return ids


def liveness_ref_set(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    next_round_id: str,
    object_kind: str,
    objects: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [
        compact_object(
            object_kind,
            item,
            run_dir=run_dir,
            run_id=run_id,
            round_id=round_id,
            next_round_id=next_round_id,
        )
        for item in objects
        if isinstance(item, dict) and object_id_for_kind(object_kind, item)
    ]


def round_closing_checklist(
    *,
    run_dir: Path,
    run_id: str,
    round_id: str,
    unresolved_refs: list[str],
    unresolved_sets: dict[str, list[dict[str, Any]]],
    round_syntheses: list[dict[str, Any]],
) -> dict[str, Any]:
    pending_source_refs = [
        maybe_text(item.get("object_ref"))
        for item in unresolved_sets.get("pending_source_acquisition_proposals", [])
        if isinstance(item, dict) and maybe_text(item.get("object_ref"))
    ]
    source_attempt_review_refs = [
        maybe_text(item.get("object_ref"))
        for item in unresolved_sets.get("source_acquisition_attempts_needing_review", [])
        if isinstance(item, dict) and maybe_text(item.get("object_ref"))
    ]
    not_ready_refs = [
        maybe_text(item.get("object_ref"))
        for item in unresolved_sets.get("not_ready_readiness_opinions", [])
        if isinstance(item, dict) and maybe_text(item.get("object_ref"))
    ]
    route_assessment_refs = [
        maybe_text(item.get("object_ref"))
        for item in unresolved_sets.get(
            "route_assessments_needing_moderator_response", []
        )
        if isinstance(item, dict) and maybe_text(item.get("object_ref"))
    ]
    return {
        "schema_version": "round-closing-checklist-v1",
        "semantics": (
            "Moderator-facing closing checklist only. It lists observed gaps and "
            "copyable commands; it does not rank work, fix the next agenda, or "
            "decide evidence acceptance."
        ),
        "moderator_decision_boundary": (
            "If unresolved refs remain, the moderator must record a round synthesis "
            "and then either request a continuation round or explicitly record why "
            "the round will not continue. Failed, blocked, receipt-only, "
            "executed-without-normalized-refs, or zero-signal acquisition attempts "
            "must first receive source-owner "
            "reflection on query/window/parameter revision, same-family follow-up "
            "skills, alternate providers, or explicit source-limit rationale. "
            "This is a procedural decision requirement, not a source ranking or "
            "agenda lock. A weak or bounded report is allowed only after the "
            "moderator records claim strength, limitations, unresolved refs, and "
            "why live actionable investigation paths are not being continued now. "
            "If route assessments record source-surface mismatch or capability "
            "gap, the moderator must acknowledge, re-route, pause, bound, or "
            "explicitly disagree before repeating the same request."
        ),
        "claim_strength_obligations": claim_strength_obligations(),
        "continuation_decision_required": bool(unresolved_refs),
        "round_synthesis_required": bool(unresolved_refs and not round_syntheses),
        "list_semantics": (
            "Items are shown in a stable presentation sequence, not importance "
            "or evidence strength order."
        ),
        "items": [
            {
                "item_id": "record-round-synthesis",
                "state": "recorded" if round_syntheses else "open",
                "observed_count": len(round_syntheses),
                "command_template": run_skill_command(
                    run_dir=run_dir,
                    run_id=run_id,
                    round_id=round_id,
                    skill_name="submit-round-synthesis",
                    actor_role="moderator",
                    contract_mode="warn",
                    skill_args=[
                        "--author-role",
                        "moderator",
                        "--synthesis-text",
                        "<round_stage_synthesis>",
                        "--stage-conclusion",
                        "<stage_conclusion>",
                        "--rationale",
                        "<moderator_synthesis_rationale>",
                        "--unresolved-object-ref",
                        "<object_kind:object_id>",
                        "--next-round-candidate-ref",
                        "<object_kind:object_id>",
                        "--provenance-json",
                        "{\"source\":\"round-closing-checklist\"}",
                    ],
                ),
            },
            {
                "item_id": "review-nonproductive-source-acquisition-attempts",
                "state": "review-required" if source_attempt_review_refs else "observed-clear",
                "attempt_refs": source_attempt_review_refs[:20],
                "owner_required_action": (
                    "The source owner records whether to revise query terms, "
                    "broaden/narrow the window or parameters, use same-family "
                    "follow-up skills, switch provider, or explicitly document "
                    "why the source family cannot continue."
                    if source_attempt_review_refs
                    else ""
                ),
                "moderator_boundary": (
                    "Do not treat failed/blocked/receipt-only/executed-without-normalized-refs/zero-signal attempts "
                    "as no-actionable-path until owner reflection is on record and "
                    "the moderator explicitly decides continuation or non-continuation."
                    if source_attempt_review_refs
                    else ""
                ),
                "reflection_surfaces": [
                    "submit-agent-position",
                    "submit-readiness-opinion",
                    "submit-evidence-request",
                    "submit-source-acquisition-proposal",
                    "submit-round-synthesis",
                ],
            },
            route_assessment_closing_item(route_assessment_refs),
            claim_strength_closing_item(
                unresolved_refs=unresolved_refs,
                source_attempt_review_refs=source_attempt_review_refs,
            ),
            {
                "item_id": "resolve-or-carry-unresolved-refs",
                "state": "open" if unresolved_refs else "observed-clear",
                "unresolved_ref_count": len(unresolved_refs),
                "observed_refs": unresolved_refs[:20],
                "moderator_required_choice": (
                    "request-continuation-round or record-explicit-non-continuation-rationale"
                    if unresolved_refs
                    else ""
                ),
                "available_paths": [
                    "carry selected refs into a continuation request",
                    "open a route-discovery continuation when route assessment says the current source surface is mismatched",
                    "record report-ready/no-actionable-path/human-paused/out-of-scope in synthesis after source-owner reflection",
                ],
            },
            {
                "item_id": "link-source-acquisition-execution",
                "state": "open" if pending_source_refs else "observed-clear",
                "pending_source_acquisition_refs": pending_source_refs[:20],
                "command_template": run_skill_command(
                    run_dir=run_dir,
                    run_id=run_id,
                    round_id=round_id,
                    skill_name="link-source-acquisition-execution",
                    actor_role="<actor_role>",
                    contract_mode="warn",
                    skill_args=[
                        "--object-id",
                        "<source_acquisition_proposal_id>",
                        "--actor-role",
                        "<actor_role>",
                        "--status-rationale",
                        "<execution_lineage_rationale>",
                        "--fetch-receipt-ref",
                        "<fetch_receipt_ref>",
                        "--normalization-receipt-ref",
                        "<normalization_receipt_ref>",
                        "--normalized-signal-ref",
                        "<normalized_signal_ref>",
                    ],
                ),
            },
            {
                "item_id": "record-readiness-or-limitation",
                "state": "open" if not_ready_refs else "available",
                "not_ready_readiness_refs": not_ready_refs[:20],
                "available_paths": [
                    "submit readiness opinion",
                    "carry unresolved limitation into round synthesis",
                ],
            },
        ],
    }


def build_round_liveness_surface(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
    next_round_id: str = "<target_round_id>",
    limit: int = 50,
) -> dict[str, Any]:
    run_dir_path = Path(run_dir).expanduser().resolve()
    warnings: list[dict[str, str]] = []
    query_limit = max(1, min(200, int(limit or 50)))

    evidence_requests, query_warnings = query_objects(
        run_dir_path,
        object_kind="evidence-request",
        run_id=run_id,
        round_id=round_id,
        limit=query_limit,
    )
    warnings.extend(query_warnings)
    source_proposals, query_warnings = query_objects(
        run_dir_path,
        object_kind="source-acquisition-proposal",
        run_id=run_id,
        round_id=round_id,
        limit=query_limit,
    )
    warnings.extend(query_warnings)
    route_assessments, query_warnings = query_objects(
        run_dir_path,
        object_kind="evidence-route-assessment",
        run_id=run_id,
        round_id=round_id,
        limit=query_limit,
    )
    warnings.extend(query_warnings)
    round_syntheses, query_warnings = query_objects(
        run_dir_path,
        object_kind="round-synthesis",
        run_id=run_id,
        round_id=round_id,
        limit=query_limit,
    )
    warnings.extend(query_warnings)
    findings, query_warnings = query_objects(
        run_dir_path,
        object_kind="finding",
        run_id=run_id,
        round_id=round_id,
        limit=query_limit,
    )
    warnings.extend(query_warnings)
    bundles, query_warnings = query_objects(
        run_dir_path,
        object_kind="evidence-bundle",
        run_id=run_id,
        round_id=round_id,
        limit=query_limit,
    )
    warnings.extend(query_warnings)
    hypotheses, query_warnings = query_objects(
        run_dir_path,
        object_kind="hypothesis",
        run_id=run_id,
        round_id=round_id,
        limit=query_limit,
    )
    warnings.extend(query_warnings)
    challenges, query_warnings = query_objects(
        run_dir_path,
        object_kind="challenge",
        run_id=run_id,
        round_id=round_id,
        limit=query_limit,
    )
    warnings.extend(query_warnings)
    board_tasks, query_warnings = query_objects(
        run_dir_path,
        object_kind="board-task",
        run_id=run_id,
        round_id=round_id,
        limit=query_limit,
    )
    warnings.extend(query_warnings)
    readiness_opinions, query_warnings = query_objects(
        run_dir_path,
        object_kind="readiness-opinion",
        run_id=run_id,
        round_id=round_id,
        limit=query_limit,
    )
    warnings.extend(query_warnings)

    referenced_findings = bundled_finding_ids(bundles)
    open_evidence_requests = [
        item for item in evidence_requests if is_open_status(item.get("status"))
    ]
    pending_source_proposals = [
        item for item in source_proposals if is_open_source_proposal(item)
    ]
    source_attempts_needing_review = [
        item for item in source_proposals if is_nonproductive_source_attempt(item)
    ]
    unbundled_findings = [
        item
        for item in findings
        if object_id_for_kind("finding", item) not in referenced_findings
    ]
    active_hypotheses = [
        item for item in hypotheses if is_open_status(item.get("status"))
    ]
    open_challenges = [
        item for item in challenges if is_open_status(item.get("status"))
    ]
    open_tasks = [
        item for item in board_tasks if is_open_status(item.get("status"))
    ]
    not_ready_readiness = [
        item for item in readiness_opinions if is_not_ready_opinion(item)
    ]
    route_assessments_needing_response = [
        item
        for item in route_assessments
        if route_assessment_needs_moderator_response(
            item,
            terminal_statuses=TERMINAL_STATUSES,
        )
    ]

    unresolved_sets = {
        "open_evidence_requests": liveness_ref_set(
            run_dir=run_dir_path,
            run_id=run_id,
            round_id=round_id,
            next_round_id=next_round_id,
            object_kind="evidence-request",
            objects=open_evidence_requests,
        ),
        "unbundled_findings": liveness_ref_set(
            run_dir=run_dir_path,
            run_id=run_id,
            round_id=round_id,
            next_round_id=next_round_id,
            object_kind="finding",
            objects=unbundled_findings,
        ),
        "pending_source_acquisition_proposals": liveness_ref_set(
            run_dir=run_dir_path,
            run_id=run_id,
            round_id=round_id,
            next_round_id=next_round_id,
            object_kind="source-acquisition-proposal",
            objects=pending_source_proposals,
        ),
        "source_acquisition_attempts_needing_review": liveness_ref_set(
            run_dir=run_dir_path,
            run_id=run_id,
            round_id=round_id,
            next_round_id=next_round_id,
            object_kind="source-acquisition-proposal",
            objects=source_attempts_needing_review,
        ),
        "route_assessments_needing_moderator_response": liveness_ref_set(
            run_dir=run_dir_path,
            run_id=run_id,
            round_id=round_id,
            next_round_id=next_round_id,
            object_kind="evidence-route-assessment",
            objects=route_assessments_needing_response,
        ),
        "open_board_tasks": liveness_ref_set(
            run_dir=run_dir_path,
            run_id=run_id,
            round_id=round_id,
            next_round_id=next_round_id,
            object_kind="board-task",
            objects=open_tasks,
        ),
        "active_hypotheses": liveness_ref_set(
            run_dir=run_dir_path,
            run_id=run_id,
            round_id=round_id,
            next_round_id=next_round_id,
            object_kind="hypothesis",
            objects=active_hypotheses,
        ),
        "not_ready_readiness_opinions": liveness_ref_set(
            run_dir=run_dir_path,
            run_id=run_id,
            round_id=round_id,
            next_round_id=next_round_id,
            object_kind="readiness-opinion",
            objects=not_ready_readiness,
        ),
        "open_challenges": liveness_ref_set(
            run_dir=run_dir_path,
            run_id=run_id,
            round_id=round_id,
            next_round_id=next_round_id,
            object_kind="challenge",
            objects=open_challenges,
        ),
    }
    unresolved_refs = unique_texts(
        [
            item.get("object_ref")
            for objects in unresolved_sets.values()
            for item in objects
            if isinstance(item, dict)
        ]
    )
    focus_payload = {
        "round_mode": "continuation",
        "primary_focus_refs": ["<object_kind:object_id>"],
        "continuation_basis": "moderator-selected unresolved refs",
        "closure_reason_if_not_continuing": "<report-ready|no-actionable-path|human-paused|out-of-scope>",
    }
    liveness_status = (
        "unresolved-refs-present"
        if unresolved_refs
        else "no-unresolved-refs-observed"
    )
    return {
        "schema_version": "round-liveness-surface-v1",
        "run_id": run_id,
        "round_id": round_id,
        "semantics": (
            "Read-only unresolved-object surface. It is coordination evidence for "
            "the moderator and agents; it does not rank, score, select sources, "
            "or force a fixed agenda."
        ),
        "ordering_semantics": (
            "Objects are shown in storage/query order only, not salience or "
            "evidence strength order."
        ),
        "handoff_semantics": (
            "Per-object handoff commands are copyable object-local templates. "
            "They do not select a source, decide evidence adoption, or require "
            "a fixed next-round agenda."
        ),
        "claim_strength_obligations": claim_strength_obligations(),
        "counts": {
            key + "_count": len(value)
            for key, value in unresolved_sets.items()
        },
        "unresolved_ref_count": len(unresolved_refs),
        "unresolved_refs": unresolved_refs,
        "unresolved_sets": unresolved_sets,
        "liveness_status": liveness_status,
        "closing_checklist": round_closing_checklist(
            run_dir=run_dir_path,
            run_id=run_id,
            round_id=round_id,
            unresolved_refs=unresolved_refs,
            unresolved_sets=unresolved_sets,
            round_syntheses=round_syntheses,
        ),
        "continuation": {
            "status": liveness_status,
            "moderator_boundary": (
                "If unresolved refs remain, the moderator must decide procedurally: "
                "request a continuation round for actionable follow-up, or record "
                "report-ready, no-actionable-path, human-paused, or out-of-scope in "
                "round synthesis. Nonproductive source attempts require owner "
                "reflection before no-actionable-path is procedurally supportable. "
                "Route assessments that record source-surface mismatch or capability "
                "gap require moderator acknowledgement, re-routing, pause, bounded "
                "report rationale, or explicit disagreement before repeating the "
                "same evidence request. "
                "Weak reports remain possible, but only with explicit claim "
                "strength, limitation, and non-continuation rationale. This does "
                "not rank sources or fix the next agenda."
            ),
            "continuation_decision_required": bool(unresolved_refs),
            "round_synthesis_required_before_continuation_decision": bool(
                unresolved_refs and not round_syntheses
            ),
            "request_open_round_command_template": kernel_command(
                "request-phase-transition",
                "--run-dir",
                str(run_dir_path),
                "--run-id",
                run_id,
                "--round-id",
                round_id,
                "--transition-kind",
                "open-investigation-round",
                "--target-round-id",
                maybe_text(next_round_id) or "<target_round_id>",
                "--source-round-id",
                round_id,
                "--request-payload-json",
                json.dumps(focus_payload, ensure_ascii=True, sort_keys=True),
                "--rationale",
                "<moderator_continuation_rationale>",
                actor_role="moderator",
            ),
            "open_round_after_approval_command_template": run_skill_command(
                run_dir=run_dir_path,
                run_id=run_id,
                round_id=maybe_text(next_round_id) or "<target_round_id>",
                skill_name="open-investigation-round",
                actor_role="moderator",
                contract_mode="warn",
                skill_args=[
                    "--source-round-id",
                    round_id,
                    "--transition-request-id",
                    "<approved_request_id>",
                    "--round-mode",
                    "continuation",
                    "--primary-focus-ref",
                    "<object_kind:object_id>",
                ],
            ),
        },
        "query_commands": {
            object_kind: kernel_command(
                "query-council-objects",
                "--run-dir",
                str(run_dir_path),
                "--object-kind",
                object_kind,
                "--run-id",
                run_id,
                "--round-id",
                round_id,
                "--pretty",
            )
            for object_kind in (
                "evidence-request",
                "round-synthesis",
                "source-acquisition-proposal",
                "evidence-route-assessment",
                "finding",
                "evidence-bundle",
                "hypothesis",
                "challenge",
                "board-task",
                "readiness-opinion",
            )
        },
        "warnings": warnings,
    }


__all__ = [
    "build_round_liveness_surface",
    "compact_object",
    "object_ref",
]
