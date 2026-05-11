from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from eco_council_runtime.objects.council import query_council_objects
from eco_council_runtime.runtime_command_hints import kernel_command, run_skill_command


TERMINAL_STATUSES = {"closed", "completed", "done", "executed", "rejected", "resolved", "retired", "withdrawn"}
READY_STATUSES = {"ready", "report-ready"}


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
                "<proposed|approved-for-execution|executed|withdrawn|rejected>",
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
    ):
        value = payload.get(field_name)
        if isinstance(value, bool) or maybe_text(value):
            compact[field_name] = value
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


def is_not_ready_opinion(payload: dict[str, Any]) -> bool:
    readiness_status = maybe_text(payload.get("readiness_status"))
    if readiness_status in READY_STATUSES:
        return False
    if bool(payload.get("sufficient_for_report_basis")):
        return False
    return True


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
        item for item in source_proposals if is_open_status(item.get("status"))
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
        "counts": {
            key + "_count": len(value)
            for key, value in unresolved_sets.items()
        },
        "unresolved_ref_count": len(unresolved_refs),
        "unresolved_refs": unresolved_refs,
        "unresolved_sets": unresolved_sets,
        "liveness_status": liveness_status,
        "continuation": {
            "status": liveness_status,
            "moderator_boundary": (
                "Moderator decides whether these refs justify continuation. If "
                "not continuing while refs remain, record report-ready, "
                "no-actionable-path, human-paused, or out-of-scope."
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
                "source-acquisition-proposal",
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
