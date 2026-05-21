from __future__ import annotations

from typing import Any

from eco_council_runtime.contracts import canonical_contract, validate_canonical_payload
from eco_council_runtime.deliberation_target_semantics import (
    normalized_deliberation_target,
    proposal_target_from_payload,
)
from eco_council_runtime.kernel.planes.deliberation_plane import (
    maybe_text,
    stable_hash,
    utc_now_iso,
)


OBJECT_KIND_PROPOSAL = "proposal"
OBJECT_KIND_REPORT_BLUEPRINT = "report-blueprint"
OBJECT_KIND_INVESTIGATION_THEME = "investigation-theme"
OBJECT_KIND_THEME_ACQUISITION_PLAN = "theme-acquisition-plan"
OBJECT_KIND_FINDING = "finding"
OBJECT_KIND_DISCUSSION_MESSAGE = "discussion-message"
OBJECT_KIND_EVIDENCE_BUNDLE = "evidence-bundle"
OBJECT_KIND_REVIEW_COMMENT = "review-comment"
OBJECT_KIND_INVESTIGATION_PLAN = "investigation-plan"
OBJECT_KIND_SUBISSUE = "subissue"
OBJECT_KIND_INVESTIGATION_SCOPE = "investigation-scope"
OBJECT_KIND_ROUND_BRIEF = "round-brief"
OBJECT_KIND_ROUND_SYNTHESIS = "round-synthesis"
OBJECT_KIND_EVIDENCE_REQUEST = "evidence-request"
OBJECT_KIND_SOURCE_ACQUISITION_PROPOSAL = "source-acquisition-proposal"
OBJECT_KIND_EVIDENCE_ROUTE_ASSESSMENT = "evidence-route-assessment"
OBJECT_KIND_AGENT_POSITION = "agent-position"
OBJECT_KIND_CONTEXT_PACKET = "context-packet"
OBJECT_KIND_CHALLENGE_DISPOSITION = "challenge-disposition"
OBJECT_KIND_HYPOTHESIS = "hypothesis"
OBJECT_KIND_CHALLENGE = "challenge"
OBJECT_KIND_BOARD_TASK = "board-task"
OBJECT_KIND_NEXT_ACTION = "next-action"
OBJECT_KIND_PROBE = "probe"
OBJECT_KIND_READINESS_OPINION = "readiness-opinion"
OBJECT_KIND_READINESS_ASSESSMENT = "readiness-assessment"
OBJECT_KIND_REPORT_BASIS_FREEZE = "report-basis-freeze"
OBJECT_KIND_DECISION_TRACE = "decision-trace"

DYNAMIC_INVESTIGATION_OBJECT_KINDS = (
    OBJECT_KIND_REPORT_BLUEPRINT,
    OBJECT_KIND_INVESTIGATION_THEME,
    OBJECT_KIND_THEME_ACQUISITION_PLAN,
    OBJECT_KIND_INVESTIGATION_PLAN,
    OBJECT_KIND_SUBISSUE,
    OBJECT_KIND_INVESTIGATION_SCOPE,
    OBJECT_KIND_ROUND_BRIEF,
    OBJECT_KIND_ROUND_SYNTHESIS,
    OBJECT_KIND_EVIDENCE_REQUEST,
    OBJECT_KIND_SOURCE_ACQUISITION_PROPOSAL,
    OBJECT_KIND_EVIDENCE_ROUTE_ASSESSMENT,
    OBJECT_KIND_AGENT_POSITION,
    OBJECT_KIND_CONTEXT_PACKET,
    OBJECT_KIND_CHALLENGE_DISPOSITION,
)

DYNAMIC_INVESTIGATION_ID_FIELDS = {
    OBJECT_KIND_REPORT_BLUEPRINT: "blueprint_id",
    OBJECT_KIND_INVESTIGATION_THEME: "theme_id",
    OBJECT_KIND_THEME_ACQUISITION_PLAN: "plan_id",
    OBJECT_KIND_INVESTIGATION_PLAN: "plan_id",
    OBJECT_KIND_SUBISSUE: "subissue_id",
    OBJECT_KIND_INVESTIGATION_SCOPE: "scope_id",
    OBJECT_KIND_ROUND_BRIEF: "brief_id",
    OBJECT_KIND_ROUND_SYNTHESIS: "synthesis_id",
    OBJECT_KIND_EVIDENCE_REQUEST: "request_id",
    OBJECT_KIND_SOURCE_ACQUISITION_PROPOSAL: "proposal_id",
    OBJECT_KIND_EVIDENCE_ROUTE_ASSESSMENT: "assessment_id",
    OBJECT_KIND_AGENT_POSITION: "position_id",
    OBJECT_KIND_CONTEXT_PACKET: "packet_id",
    OBJECT_KIND_CHALLENGE_DISPOSITION: "disposition_id",
}

DYNAMIC_INVESTIGATION_STATUS_DEFAULTS = {
    OBJECT_KIND_REPORT_BLUEPRINT: "framed",
    OBJECT_KIND_INVESTIGATION_THEME: "open",
    OBJECT_KIND_THEME_ACQUISITION_PLAN: "submitted",
    OBJECT_KIND_INVESTIGATION_PLAN: "draft",
    OBJECT_KIND_SUBISSUE: "proposed",
    OBJECT_KIND_INVESTIGATION_SCOPE: "candidate",
    OBJECT_KIND_ROUND_BRIEF: "draft",
    OBJECT_KIND_ROUND_SYNTHESIS: "recorded",
    OBJECT_KIND_EVIDENCE_REQUEST: "open",
    OBJECT_KIND_SOURCE_ACQUISITION_PROPOSAL: "proposed",
    OBJECT_KIND_EVIDENCE_ROUTE_ASSESSMENT: "recorded",
    OBJECT_KIND_AGENT_POSITION: "proposed",
    OBJECT_KIND_CONTEXT_PACKET: "materialized",
    OBJECT_KIND_CHALLENGE_DISPOSITION: "recorded",
}

FORBIDDEN_DYNAMIC_INVESTIGATION_FIELDS = (
    "blocking_if_missing",
    "candidate_source_weight",
    "confidence",
    "confidence_score",
    "heuristic",
    "heuristic_rule",
    "heuristic_rules",
    "heuristics",
    "minimum_coverage",
    "priority",
    "priority_order",
    "quality_score",
    "rank",
    "ranked_items",
    "ranking",
    "readiness_score",
    "recommended_conclusion",
    "recommended_outcome",
    "recommended_source_rank",
    "score",
    "scores",
    "source_weight",
    "sufficiency_score",
    "support_level",
    "support_score",
    "weight",
    "weights",
)

DYNAMIC_INVESTIGATION_LINEAGE_FIELDS = (
    "source_object_ids",
    "related_object_ids",
    "subissue_ids",
    "scope_ids",
    "evidence_request_ids",
    "claim_slots_supported",
    "theme_ids",
    "investigation_theme_ids",
    "theme_acquisition_plan_ids",
    "acquisition_checkpoint_ids",
    "theme_sufficiency_review_ids",
    "position_ids",
    "context_packet_ids",
    "source_acquisition_proposal_ids",
    "evidence_route_assessment_ids",
    "supersedes_object_ids",
    "response_to_ids",
    "target_refs",
    "delta_refs",
    "covered_object_refs",
    "resolved_object_refs",
    "unresolved_object_refs",
    "evidence_gap_refs",
    "next_round_candidate_refs",
    "target_evidence_request_ids",
    "route_assessment_refs",
    "capability_gap_refs",
    "rejected_route_refs",
    "fetch_receipt_refs",
    "normalization_receipt_refs",
    "normalized_signal_refs",
    "execution_artifact_refs",
)


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


def normalized_evidence_refs(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def normalized_lineage(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def normalized_provenance(
    value: Any,
    *,
    decision_source: str,
) -> dict[str, Any]:
    if isinstance(value, dict):
        normalized = dict(value)
    else:
        normalized = {}
    if decision_source and "decision_source" not in normalized:
        normalized["decision_source"] = decision_source
    return normalized


def maybe_number(value: Any) -> float | int | None:
    if isinstance(value, bool) or value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return value
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalized_text_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return unique_texts(value)


def confidence_or_default(value: Any, *, default: float = 0.7) -> float:
    parsed = maybe_number(value)
    if parsed is None:
        return float(default)
    return float(parsed)


def title_from_text(value: Any, *, prefix: str) -> str:
    text = maybe_text(value)
    if text:
        return text
    return maybe_text(prefix)


def default_deliberation_target(
    target: Any,
    *,
    round_id: str,
    target_kind: Any = "",
    target_id: Any = "",
) -> dict[str, Any]:
    resolved_target_id = maybe_text(target_id) or maybe_text(round_id)
    return normalized_deliberation_target(
        target,
        object_kind=maybe_text(target_kind) or "round",
        object_id=resolved_target_id,
        round_id=round_id,
    )


def require_non_empty_evidence_refs(
    object_kind: str,
    evidence_refs: list[Any],
) -> None:
    if evidence_refs:
        return
    raise ValueError(f"{object_kind} requires at least one evidence_ref.")


def proposal_id(
    run_id: str,
    round_id: str,
    proposal_kind: str,
    agent_role: str,
    proposal_index: int,
    rationale: str,
) -> str:
    return "proposal-" + stable_hash(
        "council-proposal",
        run_id,
        round_id,
        proposal_kind,
        agent_role,
        proposal_index,
        rationale,
    )[:12]


def finding_id(
    run_id: str,
    round_id: str,
    finding_kind: str,
    agent_role: str,
    finding_index: int,
    summary: str,
) -> str:
    return "finding-" + stable_hash(
        "finding-record",
        run_id,
        round_id,
        finding_kind,
        agent_role,
        finding_index,
        summary,
    )[:12]


def discussion_message_id(
    run_id: str,
    round_id: str,
    author_role: str,
    thread_id: str,
    message_index: int,
    message_text: str,
) -> str:
    return "discussion-message-" + stable_hash(
        "discussion-message",
        run_id,
        round_id,
        author_role,
        thread_id,
        message_index,
        message_text,
    )[:12]


def evidence_bundle_id(
    run_id: str,
    round_id: str,
    bundle_kind: str,
    agent_role: str,
    bundle_index: int,
    title: str,
) -> str:
    return "evidence-bundle-" + stable_hash(
        "evidence-bundle",
        run_id,
        round_id,
        bundle_kind,
        agent_role,
        bundle_index,
        title,
    )[:12]


def review_comment_id(
    run_id: str,
    round_id: str,
    author_role: str,
    thread_id: str,
    comment_index: int,
    comment_text: str,
) -> str:
    return "review-comment-" + stable_hash(
        "review-comment",
        run_id,
        round_id,
        author_role,
        thread_id,
        comment_index,
        comment_text,
    )[:12]


def readiness_opinion_id(
    run_id: str,
    round_id: str,
    agent_role: str,
    readiness_status: str,
    opinion_index: int,
) -> str:
    return "readiness-opinion-" + stable_hash(
        "readiness-opinion",
        run_id,
        round_id,
        agent_role,
        readiness_status,
        opinion_index,
    )[:12]


def dynamic_investigation_object_id(
    object_kind: str,
    run_id: str,
    round_id: str,
    author_role: str,
    object_index: int,
    rationale: str,
) -> str:
    normalized_kind = maybe_text(object_kind)
    return normalized_kind + "-" + stable_hash(
        "dynamic-investigation-object",
        normalized_kind,
        run_id,
        round_id,
        author_role,
        object_index,
        rationale,
    )[:12]


def reject_dynamic_investigation_heuristic_fields(
    object_kind: str,
    payload: dict[str, Any],
) -> None:
    for field_name in FORBIDDEN_DYNAMIC_INVESTIGATION_FIELDS:
        if field_name in payload:
            raise ValueError(
                f"{object_kind} cannot include heuristic/control field "
                f"`{field_name}`. Runtime dynamic investigation objects are "
                "structural envelopes only; evidence weighting and uptake stay "
                "with agents."
            )


def normalize_query_parameters(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("source-acquisition-proposal query_parameters must be a JSON object.")
    return dict(value)


def validate_source_acquisition_proposal_payload(payload: dict[str, Any]) -> None:
    source_skill = maybe_text(payload.get("source_skill"))
    if not source_skill:
        raise ValueError("source-acquisition-proposal requires source_skill.")

    from eco_council_runtime.kernel.governance.skill_registry import (  # noqa: PLC0415
        SKILL_LAYER_FETCH,
        resolve_skill_policy,
    )

    policy = resolve_skill_policy(source_skill)
    if maybe_text(policy.get("skill_layer")) != SKILL_LAYER_FETCH:
        raise ValueError(
            "source-acquisition-proposal source_skill must be a fetch skill, "
            f"got `{source_skill}`."
        )
    author_role = maybe_text(payload.get("author_role"))
    allowed_roles = (
        policy.get("allowed_roles", [])
        if isinstance(policy.get("allowed_roles"), list)
        else []
    )
    if author_role not in allowed_roles:
        raise ValueError(
            f"source-acquisition-proposal author_role `{author_role}` cannot execute "
            f"{source_skill}. Allowed roles: {', '.join(allowed_roles)}."
        )

    payload["query_parameters"] = normalize_query_parameters(
        payload.get("query_parameters")
    )
    payload["declared_side_effects"] = normalized_text_list(
        payload.get("declared_side_effects")
    )
    payload["requested_side_effect_approvals"] = normalized_text_list(
        payload.get("requested_side_effect_approvals")
    )
    undeclared = [
        value
        for value in payload["requested_side_effect_approvals"]
        if value not in payload["declared_side_effects"]
    ]
    if undeclared:
        raise ValueError(
            "source-acquisition-proposal requested_side_effect_approvals must be "
            "declared first: " + ", ".join(undeclared)
        )


def validate_theme_acquisition_plan_payload(payload: dict[str, Any]) -> None:
    author_role = maybe_text(payload.get("author_role"))
    allowed_author_roles = {"environmental-investigator", "social-investigator"}
    if author_role not in allowed_author_roles:
        raise ValueError(
            "theme-acquisition-plan must be authored or adopted by an investigator; "
            f"got author_role `{author_role or '<empty>'}`."
        )

    authoring_mode = maybe_text(payload.get("authoring_mode")).casefold()
    if authoring_mode not in {"agent-authored", "agent-adopted"}:
        raise ValueError(
            "theme-acquisition-plan authoring_mode must be `agent-authored` or "
            f"`agent-adopted`; got `{authoring_mode or '<empty>'}`."
        )

    selected_route_text = " ".join(
        [
            " ".join(normalized_text_list(payload.get("source_family_candidates"))),
            " ".join(normalized_text_list(payload.get("query_variant_plan"))),
            " ".join(normalized_text_list(payload.get("expected_denominators"))),
        ]
    ).casefold()
    if "policy_evaluation_basis" in selected_route_text or "policy evaluation basis" in selected_route_text:
        raise ValueError(
            "theme-acquisition-plan cannot treat policy_evaluation_basis as a "
            "source family, query route, denominator, or acquisition lane."
        )


def normalized_dynamic_investigation_object_payload(
    payload: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
    object_kind: str,
    object_index: int,
) -> dict[str, Any]:
    normalized = dict(payload)
    requested_kind = maybe_text(object_kind) or maybe_text(normalized.get("object_kind"))
    payload_kind = maybe_text(normalized.get("object_kind"))
    if payload_kind and requested_kind and payload_kind != requested_kind:
        raise ValueError(
            f"Dynamic investigation payload object_kind `{payload_kind}` "
            f"does not match requested object_kind `{requested_kind}`."
        )
    normalized_kind = requested_kind or payload_kind
    if normalized_kind not in DYNAMIC_INVESTIGATION_OBJECT_KINDS:
        supported = ", ".join(DYNAMIC_INVESTIGATION_OBJECT_KINDS)
        raise ValueError(
            f"Unsupported dynamic investigation object kind: "
            f"{normalized_kind or '<empty>'}. Supported kinds: {supported}."
        )
    reject_dynamic_investigation_heuristic_fields(normalized_kind, normalized)

    normalized_run_id = maybe_text(normalized.get("run_id")) or run_id
    normalized_round_id = maybe_text(normalized.get("round_id")) or round_id
    author_role = (
        maybe_text(normalized.get("author_role"))
        or maybe_text(normalized.get("agent_role"))
        or maybe_text(normalized.get("created_by_role"))
        or "moderator"
    )
    decision_source = maybe_text(normalized.get("decision_source")) or "agent-coordination"
    normalized["run_id"] = normalized_run_id
    normalized["round_id"] = normalized_round_id
    normalized["object_kind"] = normalized_kind
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["author_role"] = author_role
    normalized["agent_role"] = maybe_text(normalized.get("agent_role")) or author_role
    normalized["decision_source"] = decision_source
    normalized["status"] = (
        maybe_text(normalized.get("status"))
        or DYNAMIC_INVESTIGATION_STATUS_DEFAULTS[normalized_kind]
    )

    target = default_deliberation_target(
        normalized.get("target"),
        round_id=normalized_round_id,
        target_kind=normalized.get("target_kind"),
        target_id=normalized.get("target_id"),
    )
    normalized["target"] = target
    normalized["target_kind"] = maybe_text(target.get("object_kind")) or "round"
    normalized["target_id"] = maybe_text(target.get("object_id")) or normalized_round_id
    if normalized_kind == OBJECT_KIND_SOURCE_ACQUISITION_PROPOSAL:
        if maybe_text(normalized.get("target_evidence_request_id")) and (
            normalized["target_kind"] == "round"
            or normalized["target_id"] == normalized_round_id
        ):
            normalized["target_kind"] = OBJECT_KIND_EVIDENCE_REQUEST
            normalized["target_id"] = maybe_text(
                normalized.get("target_evidence_request_id")
            )
            normalized["target"] = default_deliberation_target(
                {},
                round_id=normalized_round_id,
                target_kind=normalized["target_kind"],
                target_id=normalized["target_id"],
            )
        if (
            normalized["target_kind"] == OBJECT_KIND_EVIDENCE_REQUEST
            and not maybe_text(normalized.get("target_evidence_request_id"))
        ):
            normalized["target_evidence_request_id"] = normalized["target_id"]
    if normalized_kind == OBJECT_KIND_EVIDENCE_ROUTE_ASSESSMENT:
        target_request_ids = normalized_text_list(
            normalized.get("target_evidence_request_ids")
        )
        if target_request_ids and (
            normalized["target_kind"] == "round"
            or normalized["target_id"] == normalized_round_id
        ):
            normalized["target_kind"] = OBJECT_KIND_EVIDENCE_REQUEST
            normalized["target_id"] = target_request_ids[0]
            normalized["target"] = default_deliberation_target(
                {},
                round_id=normalized_round_id,
                target_kind=normalized["target_kind"],
                target_id=normalized["target_id"],
            )
        if normalized["target_kind"] == OBJECT_KIND_EVIDENCE_REQUEST:
            target_request_ids = normalized_text_list(
                [normalized["target_id"], *target_request_ids]
            )
        normalized["target_evidence_request_ids"] = target_request_ids

    rationale = (
        maybe_text(normalized.get("rationale"))
        or maybe_text(normalized.get("summary"))
        or maybe_text(normalized.get("summary_text"))
        or maybe_text(normalized.get("question"))
        or maybe_text(normalized.get("claim_summary"))
        or maybe_text(normalized.get("objective"))
        or maybe_text(normalized.get("brief_text"))
        or maybe_text(normalized.get("position_text"))
        or maybe_text(normalized.get("request_text"))
        or maybe_text(normalized.get("scope_text"))
        or maybe_text(normalized.get("title"))
        or f"{normalized_kind} submitted by {author_role}"
    )
    normalized["rationale"] = rationale

    normalized["evidence_refs"] = normalized_evidence_refs(
        normalized.get("evidence_refs")
    )
    lineage_values: list[Any] = [
        *normalized_lineage(normalized.get("lineage")),
        normalized["target_id"],
    ]
    for field_name in DYNAMIC_INVESTIGATION_LINEAGE_FIELDS:
        lineage_values.extend(normalized_text_list(normalized.get(field_name)))
    supersedes_object_id = maybe_text(normalized.get("supersedes_object_id"))
    if supersedes_object_id:
        lineage_values.append(supersedes_object_id)
    normalized["lineage"] = normalized_text_list(lineage_values)
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        decision_source=decision_source,
    )

    id_field = DYNAMIC_INVESTIGATION_ID_FIELDS[normalized_kind]
    object_id = (
        maybe_text(normalized.get("object_id"))
        or maybe_text(normalized.get(id_field))
        or dynamic_investigation_object_id(
            normalized_kind,
            normalized_run_id,
            normalized_round_id,
            author_role,
            object_index,
            rationale,
        )
    )
    normalized["object_id"] = object_id
    normalized[id_field] = maybe_text(normalized.get(id_field)) or object_id
    if normalized_kind == OBJECT_KIND_SOURCE_ACQUISITION_PROPOSAL:
        validate_source_acquisition_proposal_payload(normalized)
    if normalized_kind == OBJECT_KIND_THEME_ACQUISITION_PLAN:
        validate_theme_acquisition_plan_payload(normalized)
    normalized["schema_version"] = canonical_contract(normalized_kind).schema_version
    return validate_canonical_payload(normalized_kind, normalized)


def normalized_finding_payload(
    finding: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
    finding_index: int,
) -> dict[str, Any]:
    normalized = dict(finding)
    normalized_run_id = maybe_text(normalized.get("run_id")) or run_id
    normalized_round_id = maybe_text(normalized.get("round_id")) or round_id
    decision_source = maybe_text(normalized.get("decision_source")) or "agent-investigation"
    normalized["run_id"] = normalized_run_id
    normalized["round_id"] = normalized_round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["finding_kind"] = (
        maybe_text(normalized.get("finding_kind")) or "finding"
    )
    normalized["agent_role"] = maybe_text(normalized.get("agent_role")) or "environmental-investigator"
    normalized["status"] = maybe_text(normalized.get("status")) or "submitted"
    normalized["summary"] = maybe_text(normalized.get("summary"))
    normalized["title"] = title_from_text(
        normalized.get("title") or normalized.get("summary"),
        prefix="Finding",
    )
    normalized["rationale"] = (
        maybe_text(normalized.get("rationale"))
        or maybe_text(normalized.get("summary"))
    )
    target = default_deliberation_target(
        normalized.get("target"),
        round_id=normalized_round_id,
        target_kind=normalized.get("target_kind"),
        target_id=normalized.get("target_id"),
    )
    normalized["target"] = target
    normalized["target_kind"] = (
        maybe_text(target.get("object_kind")) or "round"
    )
    normalized["target_id"] = (
        maybe_text(target.get("object_id")) or normalized_round_id
    )
    normalized["confidence"] = confidence_or_default(normalized.get("confidence"))
    normalized["decision_source"] = decision_source
    normalized["evidence_refs"] = normalized_evidence_refs(
        normalized.get("evidence_refs")
    )
    require_non_empty_evidence_refs(OBJECT_KIND_FINDING, normalized["evidence_refs"])
    normalized["basis_object_ids"] = normalized_text_list(
        normalized.get("basis_object_ids")
    )
    normalized["source_signal_ids"] = normalized_text_list(
        normalized.get("source_signal_ids")
    )
    normalized["linked_bundle_ids"] = normalized_text_list(
        normalized.get("linked_bundle_ids")
    )
    normalized["response_to_ids"] = normalized_text_list(
        normalized.get("response_to_ids")
    )
    normalized["lineage"] = normalized_text_list(
        [
            *normalized_lineage(normalized.get("lineage")),
            *normalized["basis_object_ids"],
            *normalized["source_signal_ids"],
            *normalized["linked_bundle_ids"],
            *normalized["response_to_ids"],
            normalized["target_id"],
        ]
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        decision_source=decision_source,
    )
    normalized["finding_id"] = (
        maybe_text(normalized.get("finding_id"))
        or finding_id(
            normalized_run_id,
            normalized_round_id,
            normalized["finding_kind"],
            normalized["agent_role"],
            finding_index,
            normalized["summary"],
        )
    )
    normalized["schema_version"] = canonical_contract(OBJECT_KIND_FINDING).schema_version
    return validate_canonical_payload(OBJECT_KIND_FINDING, normalized)


def normalized_discussion_message_payload(
    message: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
    message_index: int,
) -> dict[str, Any]:
    normalized = dict(message)
    normalized_run_id = maybe_text(normalized.get("run_id")) or run_id
    normalized_round_id = maybe_text(normalized.get("round_id")) or round_id
    decision_source = maybe_text(normalized.get("decision_source")) or "agent-discussion"
    normalized["run_id"] = normalized_run_id
    normalized["round_id"] = normalized_round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["author_role"] = maybe_text(normalized.get("author_role")) or "moderator"
    normalized["message_kind"] = (
        maybe_text(normalized.get("message_kind")) or "discussion"
    )
    normalized["status"] = maybe_text(normalized.get("status")) or "posted"
    target = default_deliberation_target(
        normalized.get("target"),
        round_id=normalized_round_id,
        target_kind=normalized.get("target_kind"),
        target_id=normalized.get("target_id"),
    )
    normalized["target"] = target
    normalized["target_kind"] = (
        maybe_text(target.get("object_kind")) or "round"
    )
    normalized["target_id"] = (
        maybe_text(target.get("object_id")) or normalized_round_id
    )
    normalized["message_text"] = maybe_text(normalized.get("message_text"))
    normalized["response_to_ids"] = normalized_text_list(
        normalized.get("response_to_ids")
    )
    normalized["related_object_ids"] = normalized_text_list(
        normalized.get("related_object_ids")
    )
    normalized["thread_id"] = (
        maybe_text(normalized.get("thread_id"))
        or (
            normalized["response_to_ids"][0]
            if normalized["response_to_ids"]
            else normalized["target_id"]
        )
    )
    normalized["decision_source"] = decision_source
    normalized["evidence_refs"] = normalized_evidence_refs(
        normalized.get("evidence_refs")
    )
    normalized["lineage"] = normalized_text_list(
        [
            *normalized_lineage(normalized.get("lineage")),
            normalized["thread_id"],
            normalized["target_id"],
            *normalized["response_to_ids"],
            *normalized["related_object_ids"],
        ]
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        decision_source=decision_source,
    )
    normalized["message_id"] = (
        maybe_text(normalized.get("message_id"))
        or discussion_message_id(
            normalized_run_id,
            normalized_round_id,
            normalized["author_role"],
            normalized["thread_id"],
            message_index,
            normalized["message_text"],
        )
    )
    normalized["schema_version"] = canonical_contract(
        OBJECT_KIND_DISCUSSION_MESSAGE
    ).schema_version
    return validate_canonical_payload(OBJECT_KIND_DISCUSSION_MESSAGE, normalized)


def normalized_evidence_bundle_payload(
    bundle: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
    bundle_index: int,
) -> dict[str, Any]:
    normalized = dict(bundle)
    normalized_run_id = maybe_text(normalized.get("run_id")) or run_id
    normalized_round_id = maybe_text(normalized.get("round_id")) or round_id
    decision_source = maybe_text(normalized.get("decision_source")) or "agent-investigation"
    normalized["run_id"] = normalized_run_id
    normalized["round_id"] = normalized_round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["bundle_kind"] = (
        maybe_text(normalized.get("bundle_kind")) or "evidence-bundle"
    )
    normalized["agent_role"] = maybe_text(normalized.get("agent_role")) or "moderator"
    normalized["status"] = maybe_text(normalized.get("status")) or "submitted"
    normalized["summary"] = maybe_text(normalized.get("summary"))
    normalized["title"] = title_from_text(
        normalized.get("title") or normalized.get("summary"),
        prefix="Evidence bundle",
    )
    normalized["rationale"] = (
        maybe_text(normalized.get("rationale"))
        or maybe_text(normalized.get("summary"))
    )
    target = default_deliberation_target(
        normalized.get("target"),
        round_id=normalized_round_id,
        target_kind=normalized.get("target_kind"),
        target_id=normalized.get("target_id"),
    )
    normalized["target"] = target
    normalized["target_kind"] = (
        maybe_text(target.get("object_kind")) or "round"
    )
    normalized["target_id"] = (
        maybe_text(target.get("object_id")) or normalized_round_id
    )
    normalized["confidence"] = confidence_or_default(normalized.get("confidence"))
    normalized["decision_source"] = decision_source
    normalized["evidence_refs"] = normalized_evidence_refs(
        normalized.get("evidence_refs")
    )
    require_non_empty_evidence_refs(
        OBJECT_KIND_EVIDENCE_BUNDLE,
        normalized["evidence_refs"],
    )
    normalized["basis_object_ids"] = normalized_text_list(
        normalized.get("basis_object_ids")
    )
    normalized["source_signal_ids"] = normalized_text_list(
        normalized.get("source_signal_ids")
    )
    normalized["finding_ids"] = normalized_text_list(
        normalized.get("finding_ids")
    )
    normalized["lineage"] = normalized_text_list(
        [
            *normalized_lineage(normalized.get("lineage")),
            *normalized["basis_object_ids"],
            *normalized["source_signal_ids"],
            *normalized["finding_ids"],
            normalized["target_id"],
        ]
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        decision_source=decision_source,
    )
    normalized["bundle_id"] = (
        maybe_text(normalized.get("bundle_id"))
        or evidence_bundle_id(
            normalized_run_id,
            normalized_round_id,
            normalized["bundle_kind"],
            normalized["agent_role"],
            bundle_index,
            normalized["title"],
        )
    )
    normalized["schema_version"] = canonical_contract(
        OBJECT_KIND_EVIDENCE_BUNDLE
    ).schema_version
    return validate_canonical_payload(OBJECT_KIND_EVIDENCE_BUNDLE, normalized)


def normalized_review_comment_payload(
    comment: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
    comment_index: int,
) -> dict[str, Any]:
    normalized = dict(comment)
    normalized_run_id = maybe_text(normalized.get("run_id")) or run_id
    normalized_round_id = maybe_text(normalized.get("round_id")) or round_id
    decision_source = maybe_text(normalized.get("decision_source")) or "challenger-review"
    normalized["run_id"] = normalized_run_id
    normalized["round_id"] = normalized_round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["author_role"] = maybe_text(normalized.get("author_role")) or "challenger"
    normalized["review_kind"] = (
        maybe_text(normalized.get("review_kind")) or "review"
    )
    normalized["status"] = maybe_text(normalized.get("status")) or "open"
    target = default_deliberation_target(
        normalized.get("target"),
        round_id=normalized_round_id,
        target_kind=normalized.get("target_kind"),
        target_id=normalized.get("target_id"),
    )
    normalized["target"] = target
    normalized["target_kind"] = (
        maybe_text(target.get("object_kind")) or "round"
    )
    normalized["target_id"] = (
        maybe_text(target.get("object_id")) or normalized_round_id
    )
    normalized["comment_text"] = maybe_text(normalized.get("comment_text"))
    normalized["response_to_ids"] = normalized_text_list(
        normalized.get("response_to_ids")
    )
    normalized["thread_id"] = (
        maybe_text(normalized.get("thread_id"))
        or (
            normalized["response_to_ids"][0]
            if normalized["response_to_ids"]
            else normalized["target_id"]
        )
    )
    normalized["decision_source"] = decision_source
    normalized["evidence_refs"] = normalized_evidence_refs(
        normalized.get("evidence_refs")
    )
    normalized["lineage"] = normalized_text_list(
        [
            *normalized_lineage(normalized.get("lineage")),
            normalized["thread_id"],
            normalized["target_id"],
            *normalized["response_to_ids"],
        ]
    )
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        decision_source=decision_source,
    )
    normalized["comment_id"] = (
        maybe_text(normalized.get("comment_id"))
        or review_comment_id(
            normalized_run_id,
            normalized_round_id,
            normalized["author_role"],
            normalized["thread_id"],
            comment_index,
            normalized["comment_text"],
        )
    )
    normalized["schema_version"] = canonical_contract(
        OBJECT_KIND_REVIEW_COMMENT
    ).schema_version
    return validate_canonical_payload(OBJECT_KIND_REVIEW_COMMENT, normalized)


def normalized_proposal_payload(
    proposal: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
    proposal_index: int,
) -> dict[str, Any]:
    normalized = dict(proposal)
    normalized_run_id = maybe_text(normalized.get("run_id")) or run_id
    normalized_round_id = maybe_text(normalized.get("round_id")) or round_id
    decision_source = maybe_text(normalized.get("decision_source")) or "agent-council"
    normalized["run_id"] = normalized_run_id
    normalized["round_id"] = normalized_round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["proposal_kind"] = (
        maybe_text(normalized.get("proposal_kind")) or "general-proposal"
    )
    normalized["agent_role"] = maybe_text(normalized.get("agent_role")) or "moderator"
    normalized["status"] = maybe_text(normalized.get("status")) or "open"
    target = proposal_target_from_payload(normalized)
    normalized["target"] = target
    normalized["target_kind"] = (
        maybe_text(target.get("object_kind"))
        or maybe_text(normalized.get("target_kind"))
    )
    normalized["target_id"] = (
        maybe_text(target.get("object_id"))
        or maybe_text(normalized.get("target_id"))
    )
    if maybe_text(target.get("claim_id")):
        normalized["target_claim_id"] = maybe_text(target.get("claim_id"))
    if maybe_text(target.get("hypothesis_id")):
        normalized["target_hypothesis_id"] = maybe_text(target.get("hypothesis_id"))
    if maybe_text(target.get("ticket_id")):
        normalized["target_ticket_id"] = maybe_text(target.get("ticket_id"))
    if maybe_text(target.get("task_id")):
        normalized["target_task_id"] = maybe_text(target.get("task_id"))
    normalized["rationale"] = maybe_text(normalized.get("rationale"))
    confidence = maybe_number(normalized.get("confidence"))
    if confidence is not None:
        normalized["confidence"] = confidence
    normalized["decision_source"] = decision_source
    normalized["evidence_refs"] = normalized_evidence_refs(
        normalized.get("evidence_refs")
    )
    normalized["lineage"] = normalized_lineage(normalized.get("lineage"))
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        decision_source=decision_source,
    )
    normalized["response_to_ids"] = unique_texts(
        normalized.get("response_to_ids", [])
        if isinstance(normalized.get("response_to_ids"), list)
        else []
    )
    normalized["proposal_id"] = (
        maybe_text(normalized.get("proposal_id"))
        or proposal_id(
            normalized_run_id,
            normalized_round_id,
            maybe_text(normalized.get("proposal_kind")),
            maybe_text(normalized.get("agent_role")),
            proposal_index,
            maybe_text(normalized.get("rationale")),
        )
    )
    normalized["schema_version"] = canonical_contract(OBJECT_KIND_PROPOSAL).schema_version
    return validate_canonical_payload(OBJECT_KIND_PROPOSAL, normalized)


def normalized_readiness_opinion_payload(
    opinion: dict[str, Any],
    *,
    run_id: str,
    round_id: str,
    opinion_index: int,
) -> dict[str, Any]:
    normalized = dict(opinion)
    normalized_run_id = maybe_text(normalized.get("run_id")) or run_id
    normalized_round_id = maybe_text(normalized.get("round_id")) or round_id
    decision_source = maybe_text(normalized.get("decision_source")) or "agent-council"
    normalized["run_id"] = normalized_run_id
    normalized["round_id"] = normalized_round_id
    normalized["generated_at_utc"] = (
        maybe_text(normalized.get("generated_at_utc")) or utc_now_iso()
    )
    normalized["agent_role"] = maybe_text(normalized.get("agent_role")) or "moderator"
    normalized["opinion_status"] = maybe_text(normalized.get("opinion_status")) or "submitted"
    normalized["readiness_status"] = (
        maybe_text(normalized.get("readiness_status")) or "blocked"
    )
    normalized["sufficient_for_report_basis"] = bool(
        normalized.get("sufficient_for_report_basis")
    )
    normalized["rationale"] = maybe_text(normalized.get("rationale"))
    normalized["decision_source"] = decision_source
    normalized["basis_object_ids"] = unique_texts(
        normalized.get("basis_object_ids", [])
        if isinstance(normalized.get("basis_object_ids"), list)
        else []
    )
    normalized["evidence_refs"] = normalized_evidence_refs(
        normalized.get("evidence_refs")
    )
    normalized["lineage"] = normalized_lineage(normalized.get("lineage"))
    normalized["provenance"] = normalized_provenance(
        normalized.get("provenance"),
        decision_source=decision_source,
    )
    normalized["opinion_id"] = (
        maybe_text(normalized.get("opinion_id"))
        or readiness_opinion_id(
            normalized_run_id,
            normalized_round_id,
            maybe_text(normalized.get("agent_role")),
            maybe_text(normalized.get("readiness_status")),
            opinion_index,
        )
    )
    normalized["schema_version"] = canonical_contract(
        OBJECT_KIND_READINESS_OPINION
    ).schema_version
    return validate_canonical_payload(OBJECT_KIND_READINESS_OPINION, normalized)


__all__ = (
    "OBJECT_KIND_PROPOSAL",
    "OBJECT_KIND_FINDING",
    "OBJECT_KIND_DISCUSSION_MESSAGE",
    "OBJECT_KIND_EVIDENCE_BUNDLE",
    "OBJECT_KIND_REVIEW_COMMENT",
    "OBJECT_KIND_INVESTIGATION_PLAN",
    "OBJECT_KIND_SUBISSUE",
    "OBJECT_KIND_INVESTIGATION_SCOPE",
    "OBJECT_KIND_ROUND_BRIEF",
    "OBJECT_KIND_EVIDENCE_REQUEST",
    "OBJECT_KIND_SOURCE_ACQUISITION_PROPOSAL",
    "OBJECT_KIND_EVIDENCE_ROUTE_ASSESSMENT",
    "OBJECT_KIND_AGENT_POSITION",
    "OBJECT_KIND_CONTEXT_PACKET",
    "OBJECT_KIND_CHALLENGE_DISPOSITION",
    "OBJECT_KIND_HYPOTHESIS",
    "OBJECT_KIND_CHALLENGE",
    "OBJECT_KIND_BOARD_TASK",
    "OBJECT_KIND_NEXT_ACTION",
    "OBJECT_KIND_PROBE",
    "OBJECT_KIND_READINESS_OPINION",
    "OBJECT_KIND_READINESS_ASSESSMENT",
    "OBJECT_KIND_REPORT_BASIS_FREEZE",
    "OBJECT_KIND_DECISION_TRACE",
    "DYNAMIC_INVESTIGATION_OBJECT_KINDS",
    "DYNAMIC_INVESTIGATION_ID_FIELDS",
    "DYNAMIC_INVESTIGATION_STATUS_DEFAULTS",
    "FORBIDDEN_DYNAMIC_INVESTIGATION_FIELDS",
    "DYNAMIC_INVESTIGATION_LINEAGE_FIELDS",
    "maybe_text",
    "stable_hash",
    "utc_now_iso",
    "unique_texts",
    "normalized_evidence_refs",
    "normalized_lineage",
    "normalized_provenance",
    "maybe_number",
    "normalized_text_list",
    "confidence_or_default",
    "title_from_text",
    "default_deliberation_target",
    "require_non_empty_evidence_refs",
    "proposal_id",
    "finding_id",
    "discussion_message_id",
    "evidence_bundle_id",
    "review_comment_id",
    "readiness_opinion_id",
    "dynamic_investigation_object_id",
    "reject_dynamic_investigation_heuristic_fields",
    "normalized_dynamic_investigation_object_payload",
    "normalized_finding_payload",
    "normalized_discussion_message_payload",
    "normalized_evidence_bundle_payload",
    "normalized_review_comment_payload",
    "normalized_proposal_payload",
    "normalized_readiness_opinion_payload",
)
