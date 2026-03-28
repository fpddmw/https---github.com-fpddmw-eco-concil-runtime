"""Deterministic investigation-state assembly and persistence."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from eco_council_runtime.adapters.filesystem import write_json
from eco_council_runtime.application.reporting_drafts import (
    build_hypothesis_review_from_state,
    infer_missing_evidence_types,
    investigation_review_overall_status,
)
from eco_council_runtime.controller.paths import investigation_state_path
from eco_council_runtime.domain.text import maybe_text, unique_strings

SCHEMA_VERSION = "1.0.0"


def last_update_stage(state: dict[str, Any]) -> str:
    if isinstance(state.get("investigation_review"), dict) and state.get("investigation_review"):
        return "investigation-review"
    if any(
        [
            isinstance(state.get("matching_result"), dict) and state.get("matching_result"),
            isinstance(state.get("evidence_adjudication"), dict) and state.get("evidence_adjudication"),
            state.get("cards_active"),
            state.get("isolated_active"),
            state.get("remands_open"),
        ]
    ):
        return "evidence-adjudication"
    if any(
        [
            state.get("claims"),
            state.get("observations"),
            state.get("claim_submissions_current"),
            state.get("observation_submissions_current"),
        ]
    ):
        return "curation"
    if any([state.get("claim_candidates_current"), state.get("observation_candidates_current")]):
        return "candidate-normalization"
    return "investigation-plan"


def _unique_refs(values: list[str], *, limit: int = 8) -> list[str]:
    return unique_strings([maybe_text(item) for item in values if maybe_text(item)])[:limit]


def _lookup_by_id(items: list[dict[str, Any]], key_name: str) -> dict[str, dict[str, Any]]:
    return {
        maybe_text(item.get(key_name)): item
        for item in items
        if isinstance(item, dict) and maybe_text(item.get(key_name))
    }


def _review_hypothesis_lookup(review: dict[str, Any]) -> dict[str, dict[str, Any]]:
    hypotheses = review.get("hypothesis_reviews") if isinstance(review.get("hypothesis_reviews"), list) else []
    return {
        maybe_text(item.get("hypothesis_id")): item
        for item in hypotheses
        if isinstance(item, dict) and maybe_text(item.get("hypothesis_id"))
    }


def _review_leg_lookup(review_hypothesis: dict[str, Any]) -> dict[str, dict[str, Any]]:
    legs = review_hypothesis.get("leg_reviews") if isinstance(review_hypothesis.get("leg_reviews"), list) else []
    return {
        maybe_text(item.get("leg_id")): item
        for item in legs
        if isinstance(item, dict) and maybe_text(item.get("leg_id"))
    }


def _classify_refs(
    refs: list[str],
    *,
    cards_by_id: dict[str, dict[str, Any]],
) -> tuple[list[str], list[str], list[str], list[str]]:
    support_refs: list[str] = []
    contradiction_refs: list[str] = []
    pending_refs: list[str] = []
    direct_refs: list[str] = []
    for ref in refs:
        text = maybe_text(ref)
        if text.startswith("card:"):
            card = cards_by_id.get(text.split(":", 1)[1], {})
            verdict = maybe_text(card.get("verdict"))
            if verdict == "supports":
                support_refs.append(text)
            elif verdict == "contradicts":
                contradiction_refs.append(text)
            elif verdict == "mixed":
                support_refs.append(text)
                contradiction_refs.append(text)
            else:
                pending_refs.append(text)
        elif text.startswith(("isolated:", "remand:")):
            pending_refs.append(text)
        elif text.startswith(("claim:", "observation:")):
            direct_refs.append(text)
    return _unique_refs(support_refs), _unique_refs(contradiction_refs), _unique_refs(pending_refs), _unique_refs(direct_refs)


def _evidence_gap_refs(
    refs: list[str],
    *,
    cards_by_id: dict[str, dict[str, Any]],
    remands_by_id: dict[str, dict[str, Any]],
    isolated_by_id: dict[str, dict[str, Any]],
) -> list[str]:
    gaps: list[str] = []
    for ref in refs:
        text = maybe_text(ref)
        if text.startswith("card:"):
            card = cards_by_id.get(text.split(":", 1)[1], {})
            gaps.extend(
                maybe_text(item)
                for item in card.get("gaps", [])
                if isinstance(card.get("gaps"), list) and maybe_text(item)
            )
        elif text.startswith("remand:"):
            remand = remands_by_id.get(text.split(":", 1)[1], {})
            gaps.extend(
                maybe_text(item)
                for item in remand.get("reasons", [])
                if isinstance(remand.get("reasons"), list) and maybe_text(item)
            )
        elif text.startswith("isolated:"):
            isolated = isolated_by_id.get(text.split(":", 1)[1], {})
            reason = maybe_text(isolated.get("reason"))
            if reason:
                gaps.append(reason)
    return unique_strings(gaps)


def _uncertainty_level(*, status: str, contradiction_count: int, pending_count: int, direct_count: int) -> str:
    if status == "supported" and contradiction_count == 0 and pending_count == 0:
        return "low"
    if status in {"supported", "partial"}:
        return "medium"
    if status in {"contradicted", "isolated"}:
        return "high"
    if direct_count > 0:
        return "medium"
    return "high"


def _uncertainty_reasons(
    *,
    review_notes: list[str],
    required: bool,
    status: str,
    contradiction_count: int,
    pending_count: int,
    direct_count: int,
) -> list[str]:
    reasons = [maybe_text(item) for item in review_notes if maybe_text(item)]
    if contradiction_count > 0:
        reasons.append("Contradictory evidence cards remain active for this causal leg.")
    if pending_count > 0:
        reasons.append("Isolated or remand evidence still needs follow-up for this causal leg.")
    if direct_count > 0 and status != "supported":
        reasons.append("This leg has direct auditable submissions but no fully resolved matched evidence card yet.")
    if required and status != "supported":
        reasons.append("This required leg is not yet fully supported.")
    return unique_strings(reasons)[:6]


def build_leg_state(
    *,
    state: dict[str, Any],
    hypothesis_id: str,
    profile_id: str,
    leg: dict[str, Any],
    review_leg: dict[str, Any] | None,
    stage: str,
    round_id: str,
    cards_by_id: dict[str, dict[str, Any]],
    remands_by_id: dict[str, dict[str, Any]],
    isolated_by_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    computed_review = build_hypothesis_review_from_state(
        state=state,
        hypothesis={
            "hypothesis_id": hypothesis_id,
            "statement": "",
            "chain_legs": [leg],
        },
        profile_id=profile_id,
    ).get("leg_reviews", [{}])[0]
    merged_review = computed_review if isinstance(computed_review, dict) else {}
    if isinstance(review_leg, dict):
        merged_review = {
            **merged_review,
            **review_leg,
            "evidence_refs": _unique_refs(
                [
                    *(
                        merged_review.get("evidence_refs", [])
                        if isinstance(merged_review.get("evidence_refs"), list)
                        else []
                    ),
                    *(review_leg.get("evidence_refs", []) if isinstance(review_leg.get("evidence_refs"), list) else []),
                ]
            ),
            "notes": unique_strings(
                [
                    *(
                        merged_review.get("notes", [])
                        if isinstance(merged_review.get("notes"), list)
                        else []
                    ),
                    *(review_leg.get("notes", []) if isinstance(review_leg.get("notes"), list) else []),
                ]
            ),
        }

    evidence_refs = _unique_refs(
        merged_review.get("evidence_refs", []) if isinstance(merged_review.get("evidence_refs"), list) else []
    )
    support_refs, contradiction_refs, pending_refs, direct_refs = _classify_refs(
        evidence_refs,
        cards_by_id=cards_by_id,
    )
    status = maybe_text(merged_review.get("status")) or "unresolved"
    required = bool(leg.get("required"))
    gap_values = []
    if status != "supported":
        gap_values.extend(
            maybe_text(item)
            for item in leg.get("gap_types", [])
            if isinstance(leg.get("gap_types"), list) and maybe_text(item)
        )
    gap_values.extend(
        _evidence_gap_refs(
            [*contradiction_refs, *pending_refs],
            cards_by_id=cards_by_id,
            remands_by_id=remands_by_id,
            isolated_by_id=isolated_by_id,
        )
    )
    contradiction_count = len(contradiction_refs)
    pending_count = len(pending_refs)
    direct_count = len(direct_refs)
    notes = unique_strings(merged_review.get("notes", []) if isinstance(merged_review.get("notes"), list) else [])
    return {
        "leg_id": maybe_text(leg.get("leg_id")),
        "label": maybe_text(leg.get("label")) or maybe_text(leg.get("leg_id")).replace("_", " "),
        "required": required,
        "status": status,
        "summary": maybe_text(merged_review.get("summary")),
        "support": {
            "count": len(support_refs),
            "evidence_refs": support_refs,
        },
        "contradiction": {
            "count": contradiction_count,
            "evidence_refs": contradiction_refs,
        },
        "coverage": {
            "status": status,
            "pending_ref_count": pending_count,
            "direct_ref_count": direct_count,
        },
        "remaining_gaps": unique_strings(gap_values)[:8],
        "uncertainty": {
            "level": _uncertainty_level(
                status=status,
                contradiction_count=contradiction_count,
                pending_count=pending_count,
                direct_count=direct_count,
            ),
            "reasons": _uncertainty_reasons(
                review_notes=notes,
                required=required,
                status=status,
                contradiction_count=contradiction_count,
                pending_count=pending_count,
                direct_count=direct_count,
            ),
        },
        "latest_evidence_refs": _unique_refs([*support_refs, *contradiction_refs, *pending_refs, *direct_refs]),
        "notes": notes,
        "last_update_stage": stage,
        "last_update_round_id": round_id,
    }


def build_alternative_state(
    *,
    alternative: dict[str, Any],
    stage: str,
    round_id: str,
    hypothesis_has_context: bool,
) -> dict[str, Any]:
    coverage_status = "seeded" if hypothesis_has_context else "planned"
    uncertainty_reasons = [
        "Alternative hypotheses are tracked deterministically from the investigation plan before explicit alternative-specific evidence is materialized."
    ]
    if hypothesis_has_context:
        uncertainty_reasons.append("Current evidence belongs to the parent hypothesis context and has not yet been linked to this alternative explicitly.")
    return {
        "alternative_id": maybe_text(alternative.get("alternative_id")),
        "summary": maybe_text(alternative.get("summary")),
        "statement": maybe_text(alternative.get("statement")) or maybe_text(alternative.get("summary")),
        "priority": maybe_text(alternative.get("priority")),
        "support": {
            "count": 0,
            "evidence_refs": [],
        },
        "contradiction": {
            "count": 0,
            "evidence_refs": [],
        },
        "coverage": {
            "status": coverage_status,
        },
        "remaining_gaps": unique_strings(
            alternative.get("gap_types", []) if isinstance(alternative.get("gap_types"), list) else []
        )[:8],
        "uncertainty": {
            "level": "high",
            "reasons": unique_strings(uncertainty_reasons)[:4],
        },
        "latest_evidence_refs": [],
        "last_update_stage": stage,
        "last_update_round_id": round_id,
    }


def build_hypothesis_state(
    *,
    state: dict[str, Any],
    hypothesis: dict[str, Any],
    review_hypothesis: dict[str, Any] | None,
    profile_id: str,
    stage: str,
    round_id: str,
    cards_by_id: dict[str, dict[str, Any]],
    remands_by_id: dict[str, dict[str, Any]],
    isolated_by_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    hypothesis_id = maybe_text(hypothesis.get("hypothesis_id")) or "hypothesis-001"
    computed_review = build_hypothesis_review_from_state(state=state, hypothesis=hypothesis, profile_id=profile_id)
    merged_review = computed_review if isinstance(computed_review, dict) else {}
    if isinstance(review_hypothesis, dict):
        merged_review = {
            **merged_review,
            **review_hypothesis,
            "matched_card_ids": unique_strings(
                [
                    *(
                        merged_review.get("matched_card_ids", [])
                        if isinstance(merged_review.get("matched_card_ids"), list)
                        else []
                    ),
                    *(
                        review_hypothesis.get("matched_card_ids", [])
                        if isinstance(review_hypothesis.get("matched_card_ids"), list)
                        else []
                    ),
                ]
            ),
            "isolated_entry_ids": unique_strings(
                [
                    *(
                        merged_review.get("isolated_entry_ids", [])
                        if isinstance(merged_review.get("isolated_entry_ids"), list)
                        else []
                    ),
                    *(
                        review_hypothesis.get("isolated_entry_ids", [])
                        if isinstance(review_hypothesis.get("isolated_entry_ids"), list)
                        else []
                    ),
                ]
            ),
            "remand_ids": unique_strings(
                [
                    *(
                        merged_review.get("remand_ids", [])
                        if isinstance(merged_review.get("remand_ids"), list)
                        else []
                    ),
                    *(
                        review_hypothesis.get("remand_ids", [])
                        if isinstance(review_hypothesis.get("remand_ids"), list)
                        else []
                    ),
                ]
            ),
            "notes": unique_strings(
                [
                    *(
                        merged_review.get("notes", [])
                        if isinstance(merged_review.get("notes"), list)
                        else []
                    ),
                    *(review_hypothesis.get("notes", []) if isinstance(review_hypothesis.get("notes"), list) else []),
                ]
            ),
        }
    review_leg_lookup = _review_leg_lookup(review_hypothesis if isinstance(review_hypothesis, dict) else {})
    leg_states = [
        build_leg_state(
            state=state,
            hypothesis_id=hypothesis_id,
            profile_id=profile_id,
            leg=leg,
            review_leg=review_leg_lookup.get(maybe_text(leg.get("leg_id"))),
            stage=stage,
            round_id=round_id,
            cards_by_id=cards_by_id,
            remands_by_id=remands_by_id,
            isolated_by_id=isolated_by_id,
        )
        for leg in hypothesis.get("chain_legs", [])
        if isinstance(leg, dict) and maybe_text(leg.get("leg_id"))
    ]
    overall_status = maybe_text(merged_review.get("overall_status")) or investigation_review_overall_status([computed_review])
    support_refs = _unique_refs(
        [
            ref
            for leg_state in leg_states
            for ref in leg_state.get("support", {}).get("evidence_refs", [])
            if isinstance(leg_state.get("support"), dict)
        ]
    )
    contradiction_refs = _unique_refs(
        [
            ref
            for leg_state in leg_states
            for ref in leg_state.get("contradiction", {}).get("evidence_refs", [])
            if isinstance(leg_state.get("contradiction"), dict)
        ]
    )
    latest_evidence_refs = _unique_refs(
        [ref for leg_state in leg_states for ref in leg_state.get("latest_evidence_refs", [])]
    )
    required_legs = [item for item in leg_states if bool(item.get("required"))]
    supported_required_count = sum(1 for item in required_legs if maybe_text(item.get("status")) == "supported")
    partial_leg_count = sum(1 for item in leg_states if maybe_text(item.get("status")) == "partial")
    unresolved_leg_count = sum(
        1 for item in leg_states if maybe_text(item.get("status")) in {"unresolved", "contradicted", "isolated"}
    )
    remaining_gaps = unique_strings([gap for leg_state in leg_states for gap in leg_state.get("remaining_gaps", [])])[:12]
    alternative_hypotheses = hypothesis.get("alternative_hypotheses") if isinstance(hypothesis.get("alternative_hypotheses"), list) else []
    hypothesis_has_context = bool(latest_evidence_refs or state.get("claims") or state.get("observations"))
    alternative_states = [
        build_alternative_state(
            alternative=alternative,
            stage=stage,
            round_id=round_id,
            hypothesis_has_context=hypothesis_has_context,
        )
        for alternative in alternative_hypotheses
        if isinstance(alternative, dict)
    ]
    uncertainty_level = "low" if overall_status == "supported" and not contradiction_refs else "medium" if overall_status == "partial" else "high"
    uncertainty_reasons = unique_strings(
        [
            *(merged_review.get("notes", []) if isinstance(merged_review.get("notes"), list) else []),
            *[
                reason
                for leg_state in leg_states
                for reason in leg_state.get("uncertainty", {}).get("reasons", [])
                if isinstance(leg_state.get("uncertainty"), dict)
            ],
        ]
    )[:6]
    return {
        "hypothesis_id": hypothesis_id,
        "statement": maybe_text(hypothesis.get("statement")) or maybe_text(hypothesis.get("summary")) or "Mission hypothesis",
        "summary": maybe_text(hypothesis.get("summary")) or maybe_text(hypothesis.get("statement")),
        "overall_status": overall_status,
        "support": {
            "count": len(support_refs),
            "evidence_refs": support_refs,
        },
        "contradiction": {
            "count": len(contradiction_refs),
            "evidence_refs": contradiction_refs,
        },
        "coverage": {
            "required_leg_count": len(required_legs),
            "supported_required_leg_count": supported_required_count,
            "partial_leg_count": partial_leg_count,
            "unresolved_leg_count": unresolved_leg_count,
        },
        "remaining_gaps": remaining_gaps,
        "uncertainty": {
            "level": uncertainty_level,
            "reasons": uncertainty_reasons,
        },
        "latest_evidence_refs": latest_evidence_refs,
        "legs": leg_states,
        "alternative_hypotheses": alternative_states,
        "notes": unique_strings(merged_review.get("notes", []) if isinstance(merged_review.get("notes"), list) else [])[:6],
        "last_update_stage": stage,
        "last_update_round_id": round_id,
    }


def build_investigation_state_from_round_state(state: dict[str, Any]) -> dict[str, Any]:
    mission = state["mission"]
    round_id = maybe_text(state.get("round_id"))
    plan = state.get("investigation_plan") if isinstance(state.get("investigation_plan"), dict) else {}
    profile_id = maybe_text(plan.get("profile_id")) or "local-event"
    profile_summary = maybe_text(plan.get("profile_summary"))
    hypotheses = plan.get("hypotheses") if isinstance(plan.get("hypotheses"), list) else []
    if not hypotheses:
        hypotheses = [
            {
                "hypothesis_id": "hypothesis-001",
                "statement": maybe_text(mission.get("objective")) or maybe_text(mission.get("topic")) or "Mission hypothesis",
                "summary": maybe_text(mission.get("objective")) or maybe_text(mission.get("topic")) or "Mission hypothesis",
                "chain_legs": [],
                "alternative_hypotheses": [],
            }
        ]
    review = state.get("investigation_review") if isinstance(state.get("investigation_review"), dict) else {}
    stage = last_update_stage(state)
    cards_by_id = _lookup_by_id(state.get("cards_active", []) if isinstance(state.get("cards_active"), list) else [], "evidence_id")
    remands_by_id = _lookup_by_id(state.get("remands_open", []) if isinstance(state.get("remands_open"), list) else [], "remand_id")
    isolated_by_id = _lookup_by_id(state.get("isolated_active", []) if isinstance(state.get("isolated_active"), list) else [], "isolated_id")
    review_by_hypothesis = _review_hypothesis_lookup(review)
    hypothesis_states = [
        build_hypothesis_state(
            state=state,
            hypothesis=hypothesis,
            review_hypothesis=review_by_hypothesis.get(maybe_text(hypothesis.get("hypothesis_id"))),
            profile_id=profile_id,
            stage=stage,
            round_id=round_id,
            cards_by_id=cards_by_id,
            remands_by_id=remands_by_id,
            isolated_by_id=isolated_by_id,
        )
        for hypothesis in hypotheses
        if isinstance(hypothesis, dict)
    ]
    overall_status = maybe_text(review.get("review_status")) or investigation_review_overall_status(hypothesis_states)
    remaining_gaps = infer_missing_evidence_types(
        claims=state.get("claims", []) if isinstance(state.get("claims"), list) else [],
        observations=state.get("observations", []) if isinstance(state.get("observations"), list) else [],
        evidence_cards=state.get("cards_active", []) if isinstance(state.get("cards_active"), list) else [],
    )
    open_questions = unique_strings(
        [
            *(plan.get("open_questions", []) if isinstance(plan.get("open_questions"), list) else []),
            *(review.get("open_questions", []) if isinstance(review.get("open_questions"), list) else []),
            *(
                state.get("evidence_adjudication", {}).get("open_questions", [])
                if isinstance(state.get("evidence_adjudication"), dict)
                and isinstance(state.get("evidence_adjudication", {}).get("open_questions"), list)
                else []
            ),
        ]
    )[:8]
    supported_hypothesis_count = sum(1 for item in hypothesis_states if maybe_text(item.get("overall_status")) == "supported")
    partial_hypothesis_count = sum(1 for item in hypothesis_states if maybe_text(item.get("overall_status")) == "partial")
    unresolved_hypothesis_count = sum(
        1 for item in hypothesis_states if maybe_text(item.get("overall_status")) not in {"supported", "partial"}
    )
    return {
        "schema_version": maybe_text(plan.get("schema_version")) or maybe_text(mission.get("schema_version")) or SCHEMA_VERSION,
        "state_id": f"investigation-state-{round_id}",
        "run_id": maybe_text(mission.get("run_id")),
        "round_id": round_id,
        "profile_id": profile_id,
        "profile_summary": profile_summary,
        "overall_status": overall_status,
        "last_update_stage": stage,
        "last_update_round_id": round_id,
        "artifact_presence": {
            "claim_count": len(state.get("claims", [])) if isinstance(state.get("claims"), list) else 0,
            "observation_count": len(state.get("observations", [])) if isinstance(state.get("observations"), list) else 0,
            "matched_card_count": len(state.get("cards_active", [])) if isinstance(state.get("cards_active"), list) else 0,
            "isolated_count": len(state.get("isolated_active", [])) if isinstance(state.get("isolated_active"), list) else 0,
            "remand_count": len(state.get("remands_open", [])) if isinstance(state.get("remands_open"), list) else 0,
            "has_matching_result": bool(state.get("matching_result")),
            "has_evidence_adjudication": bool(state.get("evidence_adjudication")),
            "has_investigation_review": bool(review),
        },
        "summary": {
            "hypothesis_count": len(hypothesis_states),
            "supported_hypothesis_count": supported_hypothesis_count,
            "partial_hypothesis_count": partial_hypothesis_count,
            "unresolved_hypothesis_count": unresolved_hypothesis_count,
            "alternative_count": sum(
                len(item.get("alternative_hypotheses", []))
                for item in hypothesis_states
                if isinstance(item.get("alternative_hypotheses"), list)
            ),
            "remaining_gap_count": len(remaining_gaps),
            "open_question_count": len(open_questions),
        },
        "phase_state": state.get("phase_state", {}) if isinstance(state.get("phase_state"), dict) else {},
        "remaining_gaps": remaining_gaps,
        "open_questions": open_questions,
        "hypotheses": hypothesis_states,
    }


def materialize_investigation_state(run_dir: Path, round_id: str, *, pretty: bool = True) -> dict[str, Any]:
    from eco_council_runtime.application.reporting_state import collect_round_state

    state = collect_round_state(run_dir, round_id)
    payload = build_investigation_state_from_round_state(state)
    target_path = investigation_state_path(run_dir, round_id)
    write_json(target_path, payload, pretty=pretty)
    return {
        "run_id": maybe_text(payload.get("run_id")),
        "round_id": maybe_text(payload.get("round_id")),
        "overall_status": maybe_text(payload.get("overall_status")),
        "last_update_stage": maybe_text(payload.get("last_update_stage")),
        "hypothesis_count": len(payload.get("hypotheses", [])) if isinstance(payload.get("hypotheses"), list) else 0,
        "investigation_state_path": str(target_path),
    }


__all__ = [
    "build_investigation_state_from_round_state",
    "last_update_stage",
    "materialize_investigation_state",
]
