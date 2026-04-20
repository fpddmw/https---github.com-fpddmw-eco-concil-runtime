from __future__ import annotations

from pathlib import Path
from typing import Any

from .council_objects import query_council_objects
from .phase2_fallback_common import maybe_text, unique_texts
from .phase2_proposal_actions import proposal_target

PROMOTION_PROPOSAL_DISPOSITION_SUPPORT = "support"
PROMOTION_PROPOSAL_DISPOSITION_REJECT = "reject"
PROMOTION_PROPOSAL_DISPOSITION_NEUTRAL = "neutral"
PROMOTION_TARGET_KINDS = {
    "round",
    "promotion-basis",
    "readiness-assessment",
    "promotion-gate",
    "reporting-handoff",
    "council-decision",
}
LEGACY_PROMOTION_SUPPORT_OPERATION_KINDS = {
    "prepare-promotion",
    "promote-evidence-basis",
    "finalize-round",
    "ready-for-reporting",
    "publish-council-decision",
}
SUPPORT_PROMOTION_DISPOSITION_VALUES = {
    "allow",
    "allow-promote",
    "advance",
    "finalize",
    "promote",
    "promoted",
    "publish",
    "ready",
    "ready-for-reporting",
    "support",
}
REJECT_PROMOTION_DISPOSITION_VALUES = {
    "block",
    "blocked",
    "continue",
    "continue-investigation",
    "freeze-withheld",
    "hold",
    "oppose",
    "pending-more-investigation",
    "reject",
    "rejected",
    "withhold",
    "withheld",
}


def list_items(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def maybe_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    text = maybe_text(value).lower()
    if text in {"1", "true", "yes"}:
        return True
    if text in {"0", "false", "no"}:
        return False
    return None


def load_council_proposals(
    run_dir: str | Path,
    *,
    run_id: str,
    round_id: str,
) -> list[dict[str, Any]]:
    payload = query_council_objects(
        run_dir,
        object_kind="proposal",
        run_id=run_id,
        round_id=round_id,
        limit=200,
    )
    proposals = (
        payload.get("objects", [])
        if isinstance(payload.get("objects"), list)
        else []
    )
    return [
        proposal
        for proposal in proposals
        if isinstance(proposal, dict)
        and maybe_text(proposal.get("status")) not in {"rejected", "withdrawn", "closed"}
    ]


def load_council_readiness_opinions(
    run_dir: str | Path,
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


def proposal_operation_kinds(proposal: dict[str, Any]) -> set[str]:
    return {
        maybe_text(value)
        for value in (
            proposal.get("proposal_kind"),
            proposal.get("action_kind"),
            proposal.get("proposed_action_kind"),
        )
        if maybe_text(value)
    }


def proposal_explicit_signals(
    proposal: dict[str, Any],
) -> list[dict[str, str]]:
    signals: list[dict[str, str]] = []

    promote_allowed = maybe_bool(proposal.get("promote_allowed"))
    if promote_allowed is not None:
        signals.append(
            {
                "field": "promote_allowed",
                "value": "true" if promote_allowed else "false",
                "disposition": (
                    PROMOTION_PROPOSAL_DISPOSITION_SUPPORT
                    if promote_allowed
                    else PROMOTION_PROPOSAL_DISPOSITION_REJECT
                ),
            }
        )

    for field_name in (
        "promotion_disposition",
        "promotion_status",
        "publication_readiness",
        "handoff_status",
        "moderator_status",
    ):
        value = maybe_text(proposal.get(field_name)).lower()
        if not value:
            continue
        if value in SUPPORT_PROMOTION_DISPOSITION_VALUES:
            signals.append(
                {
                    "field": field_name,
                    "value": value,
                    "disposition": PROMOTION_PROPOSAL_DISPOSITION_SUPPORT,
                }
            )
        elif value in REJECT_PROMOTION_DISPOSITION_VALUES:
            signals.append(
                {
                    "field": field_name,
                    "value": value,
                    "disposition": PROMOTION_PROPOSAL_DISPOSITION_REJECT,
                }
            )
    return signals


def proposal_relevance_reasons(
    proposal: dict[str, Any],
    *,
    round_id: str,
    basis_id: str = "",
    selected_basis_object_ids: list[str] | None = None,
) -> list[str]:
    target = proposal_target(proposal)
    target_kind = maybe_text(target.get("object_kind")) or maybe_text(
        proposal.get("target_kind")
    )
    target_id = maybe_text(target.get("object_id")) or maybe_text(
        proposal.get("target_id")
    )
    selected_basis = {
        maybe_text(item)
        for item in (selected_basis_object_ids or [])
        if maybe_text(item)
    }
    response_to_ids = set(unique_texts(list_items(proposal.get("response_to_ids"))))
    lineage = set(unique_texts(list_items(proposal.get("lineage"))))
    reasons: list[str] = []
    if target_kind in PROMOTION_TARGET_KINDS:
        reasons.append("promotion-target-kind")
    if target_id and target_id in {maybe_text(round_id), maybe_text(basis_id)}:
        reasons.append("promotion-target-id")
    if target_id and target_id in selected_basis:
        reasons.append("selected-basis-target")
    if selected_basis.intersection(response_to_ids):
        reasons.append("basis-response-link")
    if selected_basis.intersection(lineage):
        reasons.append("basis-lineage-link")
    if proposal_explicit_signals(proposal):
        reasons.append("explicit-promotion-signal")
    return unique_texts(reasons)


def resolve_promotion_proposal(
    proposal: dict[str, Any],
    *,
    round_id: str,
    basis_id: str = "",
    selected_basis_object_ids: list[str] | None = None,
) -> dict[str, Any]:
    proposal_id = maybe_text(proposal.get("proposal_id"))
    target = proposal_target(proposal)
    target_kind = maybe_text(target.get("object_kind")) or maybe_text(
        proposal.get("target_kind")
    )
    target_id = maybe_text(target.get("object_id")) or maybe_text(
        proposal.get("target_id")
    )
    action_kinds = sorted(proposal_operation_kinds(proposal))
    relevance_reasons = proposal_relevance_reasons(
        proposal,
        round_id=round_id,
        basis_id=basis_id,
        selected_basis_object_ids=selected_basis_object_ids,
    )
    explicit_signals = proposal_explicit_signals(proposal)
    support_signals = [
        signal
        for signal in explicit_signals
        if signal.get("disposition") == PROMOTION_PROPOSAL_DISPOSITION_SUPPORT
    ]
    reject_signals = [
        signal
        for signal in explicit_signals
        if signal.get("disposition") == PROMOTION_PROPOSAL_DISPOSITION_REJECT
    ]

    disposition = PROMOTION_PROPOSAL_DISPOSITION_NEUTRAL
    resolution_mode = "not-promotion-relevant"
    if support_signals and reject_signals:
        resolution_mode = "explicit-signal-conflict"
    elif support_signals:
        disposition = PROMOTION_PROPOSAL_DISPOSITION_SUPPORT
        resolution_mode = f"explicit:{maybe_text(support_signals[0].get('field'))}"
    elif reject_signals:
        disposition = PROMOTION_PROPOSAL_DISPOSITION_REJECT
        resolution_mode = f"explicit:{maybe_text(reject_signals[0].get('field'))}"
    elif action_kinds.intersection(LEGACY_PROMOTION_SUPPORT_OPERATION_KINDS):
        disposition = PROMOTION_PROPOSAL_DISPOSITION_SUPPORT
        resolution_mode = "legacy-operation-compatibility"
        if not relevance_reasons:
            relevance_reasons = ["legacy-support-without-target"]
    elif relevance_reasons:
        resolution_mode = "promotion-neutral"

    is_relevant = disposition != PROMOTION_PROPOSAL_DISPOSITION_NEUTRAL or bool(
        relevance_reasons
    )
    return {
        "proposal_id": proposal_id,
        "proposal_kind": maybe_text(proposal.get("proposal_kind")),
        "action_kinds": action_kinds,
        "target_kind": target_kind,
        "target_id": target_id,
        "disposition": disposition,
        "is_relevant": is_relevant,
        "resolution_mode": resolution_mode,
        "relevance_reasons": relevance_reasons,
        "explicit_signals": explicit_signals,
    }


def resolve_promotion_council_inputs(
    proposals: list[dict[str, Any]] | None,
    opinions: list[dict[str, Any]] | None,
    *,
    readiness_status: str,
    allow_non_ready: bool = False,
    round_id: str = "",
    basis_id: str = "",
    selected_basis_object_ids: list[str] | None = None,
) -> dict[str, Any]:
    proposal_resolutions = [
        resolve_promotion_proposal(
            proposal,
            round_id=round_id,
            basis_id=basis_id,
            selected_basis_object_ids=selected_basis_object_ids,
        )
        for proposal in (proposals or [])
        if isinstance(proposal, dict)
    ]
    relevant_proposal_resolutions = [
        resolution
        for resolution in proposal_resolutions
        if bool(resolution.get("is_relevant"))
    ]
    supporting_proposal_ids = unique_texts(
        [
            resolution.get("proposal_id")
            for resolution in relevant_proposal_resolutions
            if resolution.get("disposition")
            == PROMOTION_PROPOSAL_DISPOSITION_SUPPORT
        ]
    )
    rejected_proposal_ids = unique_texts(
        [
            resolution.get("proposal_id")
            for resolution in relevant_proposal_resolutions
            if resolution.get("disposition")
            == PROMOTION_PROPOSAL_DISPOSITION_REJECT
        ]
    )
    neutral_proposal_ids = unique_texts(
        [
            resolution.get("proposal_id")
            for resolution in relevant_proposal_resolutions
            if resolution.get("disposition")
            == PROMOTION_PROPOSAL_DISPOSITION_NEUTRAL
        ]
    )

    ready_opinions = [
        opinion
        for opinion in (opinions or [])
        if isinstance(opinion, dict) and readiness_bucket(opinion) == "ready"
    ]
    blocked_opinions = [
        opinion
        for opinion in (opinions or [])
        if isinstance(opinion, dict) and readiness_bucket(opinion) == "blocked"
    ]
    needs_more_data_opinions = [
        opinion
        for opinion in (opinions or [])
        if isinstance(opinion, dict) and readiness_bucket(opinion) == "needs-more-data"
    ]

    gate_allows_promotion = maybe_text(readiness_status) == "ready" or bool(
        allow_non_ready
    )
    council_veto_active = bool(rejected_proposal_ids)
    promotion_status = (
        "promoted" if gate_allows_promotion and not council_veto_active else "withheld"
    )
    promote_allowed = promotion_status == "promoted"
    gate_status = "allow-promote" if promote_allowed else "freeze-withheld"

    if promote_allowed:
        supporting_opinion_ids = unique_texts(
            [opinion.get("opinion_id") for opinion in ready_opinions]
        )
        rejected_opinion_ids = unique_texts(
            [
                opinion.get("opinion_id")
                for opinion in [*blocked_opinions, *needs_more_data_opinions]
            ]
        )
    else:
        supporting_opinion_ids = unique_texts(
            [
                opinion.get("opinion_id")
                for opinion in [*blocked_opinions, *needs_more_data_opinions]
            ]
        )
        rejected_opinion_ids = unique_texts(
            [opinion.get("opinion_id") for opinion in ready_opinions]
        )

    if rejected_proposal_ids and supporting_proposal_ids:
        promotion_resolution_mode = "council-conflict-veto"
        promotion_resolution_reasons = [
            (
                f"Council proposals are split across {len(supporting_proposal_ids)} "
                f"supporting and {len(rejected_proposal_ids)} withholding positions, "
                "so promotion stays withheld."
            )
        ]
    elif rejected_proposal_ids:
        promotion_resolution_mode = "council-veto"
        promotion_resolution_reasons = [
            (
                f"{len(rejected_proposal_ids)} council proposals explicitly withhold "
                "promotion."
            )
        ]
    elif supporting_proposal_ids and promote_allowed:
        promotion_resolution_mode = "gate-passed-with-council-support"
        promotion_resolution_reasons = [
            (
                f"{len(supporting_proposal_ids)} council proposals explicitly support "
                "promotion and the current gate allows it."
            )
        ]
    elif supporting_proposal_ids:
        if allow_non_ready and maybe_text(readiness_status) != "ready":
            promotion_resolution_mode = "allow-non-ready-with-council-support"
            promotion_resolution_reasons = [
                (
                    f"{len(supporting_proposal_ids)} council proposals support promotion "
                    "and the operator override keeps promotion enabled despite a non-ready "
                    "readiness assessment."
                )
            ]
        else:
            promotion_resolution_mode = "council-support-blocked-by-gate"
            promotion_resolution_reasons = [
                (
                    f"{len(supporting_proposal_ids)} council proposals support promotion, "
                    f"but the readiness gate remains {maybe_text(readiness_status) or 'blocked'}."
                )
            ]
    elif opinions:
        promotion_resolution_mode = "readiness-opinion-gate"
        promotion_resolution_reasons = [
            (
                "Promotion follows the current readiness-opinion aggregate "
                f"({maybe_text(readiness_status) or 'blocked'})."
            )
        ]
    else:
        promotion_resolution_mode = "fallback-readiness-gate"
        promotion_resolution_reasons = [
            (
                "Promotion falls back to the current readiness gate because no "
                "promotion-relevant council proposals were resolved."
            )
        ]

    proposal_resolution_mode_counts: dict[str, int] = {}
    for resolution in relevant_proposal_resolutions:
        key = maybe_text(resolution.get("resolution_mode")) or "unknown"
        proposal_resolution_mode_counts[key] = (
            proposal_resolution_mode_counts.get(key, 0) + 1
        )

    council_input_counts = {
        "proposal_count": len([proposal for proposal in (proposals or []) if isinstance(proposal, dict)]),
        "relevant_proposal_count": len(relevant_proposal_resolutions),
        "supporting_proposal_count": len(supporting_proposal_ids),
        "rejected_proposal_count": len(rejected_proposal_ids),
        "neutral_proposal_count": len(neutral_proposal_ids),
        "opinion_count": len([opinion for opinion in (opinions or []) if isinstance(opinion, dict)]),
        "ready_opinion_count": len(ready_opinions),
        "blocked_opinion_count": len(blocked_opinions),
        "needs_more_data_opinion_count": len(needs_more_data_opinions),
        "supporting_opinion_count": len(supporting_opinion_ids),
        "rejected_opinion_count": len(rejected_opinion_ids),
    }

    return {
        "promotion_status": promotion_status,
        "promote_allowed": promote_allowed,
        "gate_status": gate_status,
        "decision_source": (
            "agent-council"
            if relevant_proposal_resolutions
            or [opinion for opinion in (opinions or []) if isinstance(opinion, dict)]
            else "policy-fallback"
        ),
        "promotion_resolution_mode": promotion_resolution_mode,
        "promotion_resolution_reasons": promotion_resolution_reasons,
        "supporting_proposal_ids": supporting_proposal_ids,
        "rejected_proposal_ids": rejected_proposal_ids,
        "neutral_proposal_ids": neutral_proposal_ids,
        "supporting_opinion_ids": supporting_opinion_ids,
        "rejected_opinion_ids": rejected_opinion_ids,
        "proposal_resolution_records": relevant_proposal_resolutions,
        "proposal_resolution_mode_counts": proposal_resolution_mode_counts,
        "council_input_counts": council_input_counts,
    }


__all__ = [
    "PROMOTION_PROPOSAL_DISPOSITION_NEUTRAL",
    "PROMOTION_PROPOSAL_DISPOSITION_REJECT",
    "PROMOTION_PROPOSAL_DISPOSITION_SUPPORT",
    "load_council_proposals",
    "load_council_readiness_opinions",
    "proposal_operation_kinds",
    "readiness_bucket",
    "resolve_promotion_council_inputs",
    "resolve_promotion_proposal",
]
