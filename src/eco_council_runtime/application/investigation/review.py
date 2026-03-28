"""Investigation-review builders extracted from reporting_drafts."""

from __future__ import annotations

from typing import Any

from eco_council_runtime.application.reporting.common import infer_missing_evidence_types
from eco_council_runtime.application.reporting.recommendations import prioritized_recommendations_from_state
from eco_council_runtime.application.reporting_state import (
    matching_executed_for_state,
    mission_run_id,
    state_auditable_submissions,
)
from eco_council_runtime.application.reporting_views import observation_metric_family
from eco_council_runtime.domain.contract_bridge import (
    resolve_schema_version,
    validate_payload_or_raise as validate_payload,
)
from eco_council_runtime.domain.text import maybe_text, truncate_text, unique_strings

SCHEMA_VERSION = resolve_schema_version("1.0.0")
INVESTIGATION_LEG_METRIC_FAMILIES: dict[str, dict[str, set[str]]] = {
    "local-event": {
        "impact": {"air-quality", "fire-detection", "meteorology", "hydrology", "soil", "other"},
        "public_interpretation": set(),
    },
    "smoke-transport": {
        "source": {"fire-detection"},
        "mechanism": {"meteorology"},
        "impact": {"air-quality"},
        "public_interpretation": set(),
    },
    "flood-upstream": {
        "source": {"meteorology", "hydrology"},
        "mechanism": {"meteorology", "hydrology"},
        "impact": {"hydrology"},
        "public_interpretation": set(),
    },
}


def investigation_leg_metric_families(profile_id: str, leg_id: str, leg: dict[str, Any] | None = None) -> set[str]:
    if isinstance(leg, dict):
        explicit = {
            maybe_text(item)
            for item in (leg.get("metric_families") if isinstance(leg.get("metric_families"), list) else [])
            if maybe_text(item)
        }
        if explicit:
            return explicit
    families = INVESTIGATION_LEG_METRIC_FAMILIES.get(profile_id, {})
    return set(families.get(leg_id, set()))


def summarize_investigation_leg_status(*, leg_label: str, status: str, matched_count: int, pending_count: int) -> str:
    label = maybe_text(leg_label) or "This leg"
    if status == "supported":
        return f"{label} is covered by {matched_count} matched evidence card(s)."
    if status == "partial":
        if pending_count > 0:
            return f"{label} is only partially covered; {pending_count} isolated/remand item(s) still need follow-up."
        return f"{label} has auditable signals but is not yet strongly resolved."
    if status == "contradicted":
        return f"{label} is currently contradicted by the available evidence."
    if status == "isolated":
        return f"{label} is represented only by isolated evidence and has not been integrated into a stronger chain."
    return f"{label} is not yet covered by the current auditable evidence."


def investigation_review_overall_status(hypothesis_reviews: list[dict[str, Any]], *, blocked: bool = False) -> str:
    if blocked:
        return "blocked"
    statuses = [maybe_text(item.get("overall_status")) for item in hypothesis_reviews if isinstance(item, dict)]
    if statuses and all(status == "supported" for status in statuses):
        return "supported"
    if any(status in {"supported", "partial"} for status in statuses):
        return "partial"
    return "unresolved"


def record_matches_hypothesis_leg(record: dict[str, Any], *, hypothesis_id: str, leg_id: str) -> bool:
    record_hypothesis_id = maybe_text(record.get("hypothesis_id"))
    if hypothesis_id and record_hypothesis_id and record_hypothesis_id != hypothesis_id:
        return False
    record_leg_id = maybe_text(record.get("leg_id"))
    if leg_id and record_leg_id and record_leg_id != leg_id:
        return False
    return True


def record_has_investigation_tags(record: dict[str, Any]) -> bool:
    return bool(maybe_text(record.get("hypothesis_id")) or maybe_text(record.get("leg_id")))


def observation_supports_investigation_leg(
    observation: dict[str, Any],
    *,
    hypothesis_id: str,
    leg_id: str,
    relevant_families: set[str],
) -> bool:
    if record_has_investigation_tags(observation):
        return record_matches_hypothesis_leg(observation, hypothesis_id=hypothesis_id, leg_id=leg_id)

    component_roles = observation.get("component_roles")
    explicit_component_seen = False
    if isinstance(component_roles, list):
        for component in component_roles:
            if not isinstance(component, dict):
                continue
            component_record = {
                "hypothesis_id": maybe_text(component.get("hypothesis_id")),
                "leg_id": maybe_text(component.get("leg_id")),
            }
            if not record_has_investigation_tags(component_record):
                continue
            explicit_component_seen = True
            if record_matches_hypothesis_leg(component_record, hypothesis_id=hypothesis_id, leg_id=leg_id):
                return True
        if explicit_component_seen:
            return False
        for component in component_roles:
            if not isinstance(component, dict):
                continue
            component_metric = maybe_text(component.get("metric")) or maybe_text(observation.get("metric"))
            if observation_metric_family(component_metric) in relevant_families:
                return True
    return observation_metric_family(observation.get("metric")) in relevant_families


def build_leg_review_from_state(
    *,
    state: dict[str, Any],
    profile_id: str,
    leg: dict[str, Any],
    hypothesis_id: str = "",
) -> dict[str, Any]:
    leg_id = maybe_text(leg.get("leg_id"))
    leg_label = maybe_text(leg.get("label")) or leg_id.replace("_", " ")
    region_hint = maybe_text(leg.get("region_hint"))
    success_criteria = maybe_text(leg.get("success_criteria"))
    evidence_focus = [maybe_text(item) for item in leg.get("evidence_focus", []) if maybe_text(item)] if isinstance(leg.get("evidence_focus"), list) else []
    relevant_claim_types = {
        maybe_text(item)
        for item in (leg.get("claim_types") if isinstance(leg.get("claim_types"), list) else [])
        if maybe_text(item)
    }
    cards = state.get("cards_active", []) if isinstance(state.get("cards_active"), list) else []
    isolated_entries = state.get("isolated_active", []) if isinstance(state.get("isolated_active"), list) else []
    remand_entries = state.get("remands_open", []) if isinstance(state.get("remands_open"), list) else []
    observations = state.get("observations", []) if isinstance(state.get("observations"), list) else []
    claim_lookup = {
        maybe_text(item.get("claim_id")): item
        for item in state.get("claims", [])
        if isinstance(item, dict) and maybe_text(item.get("claim_id"))
    }
    for submission in state_auditable_submissions(state, "sociologist"):
        claim_id = maybe_text(submission.get("claim_id"))
        if claim_id:
            claim_lookup.setdefault(claim_id, submission)
    observation_lookup = {
        maybe_text(item.get("observation_id")): item
        for item in observations
        if isinstance(item, dict) and maybe_text(item.get("observation_id"))
    }
    for submission in state_auditable_submissions(state, "environmentalist"):
        observation_id = maybe_text(submission.get("observation_id"))
        if observation_id:
            observation_lookup.setdefault(observation_id, submission)

    matched_refs: list[str] = []
    isolated_refs: list[str] = []
    remand_refs: list[str] = []
    direct_refs: list[str] = []

    if leg_id == "public_interpretation":
        for card in cards:
            if not isinstance(card, dict):
                continue
            claim_id = maybe_text(card.get("claim_id"))
            claim_record = claim_lookup.get(claim_id, {})
            if not record_matches_hypothesis_leg(claim_record, hypothesis_id=hypothesis_id, leg_id=leg_id):
                continue
            claim_type = maybe_text(claim_record.get("claim_type"))
            if relevant_claim_types and claim_type and claim_type not in relevant_claim_types:
                continue
            if claim_id and maybe_text(card.get("evidence_id")):
                matched_refs.append(f"card:{maybe_text(card.get('evidence_id'))}")
        for entry in isolated_entries:
            if not isinstance(entry, dict):
                continue
            if maybe_text(entry.get("entity_kind")) != "claim":
                continue
            claim_record = claim_lookup.get(maybe_text(entry.get("entity_id")), {})
            if not record_matches_hypothesis_leg(claim_record, hypothesis_id=hypothesis_id, leg_id=leg_id):
                continue
            claim_type = maybe_text(claim_record.get("claim_type"))
            if relevant_claim_types and claim_type and claim_type not in relevant_claim_types:
                continue
            if maybe_text(entry.get("isolated_id")):
                isolated_refs.append(f"isolated:{maybe_text(entry.get('isolated_id'))}")
        for entry in remand_entries:
            if not isinstance(entry, dict):
                continue
            if maybe_text(entry.get("entity_kind")) != "claim":
                continue
            claim_record = claim_lookup.get(maybe_text(entry.get("entity_id")), {})
            if not record_matches_hypothesis_leg(claim_record, hypothesis_id=hypothesis_id, leg_id=leg_id):
                continue
            claim_type = maybe_text(claim_record.get("claim_type"))
            if relevant_claim_types and claim_type and claim_type not in relevant_claim_types:
                continue
            if maybe_text(entry.get("remand_id")):
                remand_refs.append(f"remand:{maybe_text(entry.get('remand_id'))}")
        for submission in state_auditable_submissions(state, "sociologist"):
            claim_id = maybe_text(submission.get("claim_id"))
            if not record_matches_hypothesis_leg(submission, hypothesis_id=hypothesis_id, leg_id=leg_id):
                continue
            claim_type = maybe_text(submission.get("claim_type"))
            if relevant_claim_types and claim_type and claim_type not in relevant_claim_types:
                continue
            if claim_id:
                direct_refs.append(f"claim:{claim_id}")
    else:
        relevant_families = investigation_leg_metric_families(profile_id, leg_id, leg)
        for card in cards:
            if not isinstance(card, dict):
                continue
            if not record_matches_hypothesis_leg(card, hypothesis_id=hypothesis_id, leg_id=""):
                continue
            observation_ids = [
                maybe_text(item)
                for item in card.get("observation_ids", [])
                if maybe_text(item)
            ]
            if any(
                observation_supports_investigation_leg(
                    observation_lookup.get(observation_id, {}),
                    hypothesis_id=hypothesis_id,
                    leg_id=leg_id,
                    relevant_families=relevant_families,
                )
                for observation_id in observation_ids
            ):
                evidence_id = maybe_text(card.get("evidence_id"))
                if evidence_id:
                    matched_refs.append(f"card:{evidence_id}")
        for entry in isolated_entries:
            if not isinstance(entry, dict):
                continue
            if maybe_text(entry.get("entity_kind")) != "observation":
                continue
            observation = observation_lookup.get(maybe_text(entry.get("entity_id")))
            if observation is None:
                continue
            if observation_supports_investigation_leg(
                observation,
                hypothesis_id=hypothesis_id,
                leg_id=leg_id,
                relevant_families=relevant_families,
            ) and maybe_text(entry.get("isolated_id")):
                isolated_refs.append(f"isolated:{maybe_text(entry.get('isolated_id'))}")
        for entry in remand_entries:
            if not isinstance(entry, dict):
                continue
            if maybe_text(entry.get("entity_kind")) != "observation":
                continue
            observation = observation_lookup.get(maybe_text(entry.get("entity_id")))
            if observation is None:
                continue
            if observation_supports_investigation_leg(
                observation,
                hypothesis_id=hypothesis_id,
                leg_id=leg_id,
                relevant_families=relevant_families,
            ) and maybe_text(entry.get("remand_id")):
                remand_refs.append(f"remand:{maybe_text(entry.get('remand_id'))}")
        for submission in state_auditable_submissions(state, "environmentalist"):
            observation_id = maybe_text(submission.get("observation_id"))
            if not observation_id:
                continue
            if observation_supports_investigation_leg(
                submission,
                hypothesis_id=hypothesis_id,
                leg_id=leg_id,
                relevant_families=relevant_families,
            ):
                direct_refs.append(f"observation:{observation_id}")

    matched_count = len(matched_refs)
    pending_count = len(isolated_refs) + len(remand_refs)
    if matched_count > 0:
        status = "supported"
    elif pending_count > 0 or direct_refs:
        status = "partial"
    else:
        status = "unresolved"
    notes: list[str] = []
    if status == "unresolved" and bool(leg.get("required")):
        notes.append(f"The required leg {leg_id} still lacks auditable coverage in this round.")
    if status != "supported" and success_criteria:
        notes.append(f"Success criterion: {success_criteria}")
    if status == "unresolved" and evidence_focus:
        notes.append(f"Evidence focus: {evidence_focus[0]}")
    if status in {"partial", "unresolved"} and region_hint:
        notes.append(f"Region hint: {region_hint}")
    evidence_refs = unique_strings(matched_refs + isolated_refs + remand_refs + direct_refs)[:8]
    return {
        "leg_id": leg_id,
        "status": status,
        "summary": summarize_investigation_leg_status(
            leg_label=leg_label,
            status=status,
            matched_count=matched_count,
            pending_count=pending_count,
        ),
        "evidence_refs": evidence_refs,
        "notes": unique_strings(notes),
    }


def build_hypothesis_review_from_state(*, state: dict[str, Any], hypothesis: dict[str, Any], profile_id: str) -> dict[str, Any]:
    leg_reviews = [
        build_leg_review_from_state(
            state=state,
            profile_id=profile_id,
            leg=leg,
            hypothesis_id=maybe_text(hypothesis.get("hypothesis_id")),
        )
        for leg in hypothesis.get("chain_legs", [])
        if isinstance(leg, dict) and maybe_text(leg.get("leg_id"))
    ]
    required_leg_ids = {
        maybe_text(leg.get("leg_id"))
        for leg in hypothesis.get("chain_legs", [])
        if isinstance(leg, dict) and bool(leg.get("required")) and maybe_text(leg.get("leg_id"))
    }
    statuses_by_leg = {
        maybe_text(item.get("leg_id")): maybe_text(item.get("status"))
        for item in leg_reviews
        if isinstance(item, dict) and maybe_text(item.get("leg_id"))
    }
    if required_leg_ids and all(statuses_by_leg.get(leg_id) == "supported" for leg_id in required_leg_ids):
        overall_status = "supported"
    elif any(statuses_by_leg.get(leg_id) in {"supported", "partial"} for leg_id in required_leg_ids):
        overall_status = "partial"
    else:
        overall_status = "unresolved"

    matched_card_ids: list[str] = []
    isolated_entry_ids: list[str] = []
    remand_ids: list[str] = []
    for leg_review in leg_reviews:
        for ref in leg_review.get("evidence_refs", []):
            text = maybe_text(ref)
            if text.startswith("card:"):
                matched_card_ids.append(text.split(":", 1)[1])
            elif text.startswith("isolated:"):
                isolated_entry_ids.append(text.split(":", 1)[1])
            elif text.startswith("remand:"):
                remand_ids.append(text.split(":", 1)[1])

    unresolved_required = [
        leg_id
        for leg_id in required_leg_ids
        if statuses_by_leg.get(leg_id) not in {"supported"}
    ]
    notes: list[str] = []
    if unresolved_required:
        notes.append("Required legs still unresolved or partial: " + ", ".join(sorted(unresolved_required)) + ".")
    return {
        "hypothesis_id": maybe_text(hypothesis.get("hypothesis_id")) or "hypothesis-001",
        "statement": maybe_text(hypothesis.get("statement")) or maybe_text(hypothesis.get("summary")) or "Mission hypothesis",
        "overall_status": overall_status,
        "matched_card_ids": unique_strings(matched_card_ids),
        "isolated_entry_ids": unique_strings(isolated_entry_ids),
        "remand_ids": unique_strings(remand_ids),
        "leg_reviews": leg_reviews,
        "notes": unique_strings(notes),
    }


def build_investigation_review_draft_from_state(state: dict[str, Any]) -> dict[str, Any]:
    mission = state["mission"]
    round_id = state["round_id"]
    if not matching_executed_for_state(state):
        raise ValueError("Investigation review requires matching/adjudication artifacts to exist first.")
    matching_result = state.get("matching_result", {}) if isinstance(state.get("matching_result"), dict) else {}
    evidence_adjudication = state.get("evidence_adjudication", {}) if isinstance(state.get("evidence_adjudication"), dict) else {}
    matching_adjudication = state.get("matching_adjudication", {}) if isinstance(state.get("matching_adjudication"), dict) else {}
    plan = state.get("investigation_plan") if isinstance(state.get("investigation_plan"), dict) else {}
    if not isinstance(plan, dict) or not plan:
        plan = {"profile_id": "local-event", "hypotheses": []}
    profile_id = maybe_text(plan.get("profile_id")) or "local-event"
    hypotheses = plan.get("hypotheses") if isinstance(plan.get("hypotheses"), list) else []
    if not hypotheses:
        hypotheses = [
            {
                "hypothesis_id": "hypothesis-001",
                "statement": maybe_text(mission.get("objective")) or maybe_text(mission.get("topic")) or "Mission hypothesis",
                "chain_legs": [],
            }
        ]
    hypothesis_reviews = [
        build_hypothesis_review_from_state(state=state, hypothesis=hypothesis, profile_id=profile_id)
        for hypothesis in hypotheses
        if isinstance(hypothesis, dict)
    ]
    review_status = investigation_review_overall_status(hypothesis_reviews)
    matched_card_ids = unique_strings(
        [
            maybe_text(item.get("evidence_id"))
            for item in state.get("cards_active", [])
            if isinstance(item, dict) and maybe_text(item.get("evidence_id"))
        ]
    )
    isolated_entry_ids = unique_strings(
        [
            maybe_text(item.get("isolated_id"))
            for item in state.get("isolated_active", [])
            if isinstance(item, dict) and maybe_text(item.get("isolated_id"))
        ]
    )
    remand_ids = unique_strings(
        [
            maybe_text(item.get("remand_id"))
            for item in state.get("remands_open", [])
            if isinstance(item, dict) and maybe_text(item.get("remand_id"))
        ]
    )
    missing_types = infer_missing_evidence_types(
        claims=state.get("claims", []) if isinstance(state.get("claims"), list) else [],
        observations=state.get("observations", []) if isinstance(state.get("observations"), list) else [],
        evidence_cards=state.get("cards_active", []) if isinstance(state.get("cards_active"), list) else [],
    )
    recommendation_inputs = [
        item
        for item in [matching_adjudication, evidence_adjudication]
        if isinstance(item, dict) and item
    ]
    recommendations = prioritized_recommendations_from_state(
        state=state,
        reports=recommendation_inputs,
        missing_types=missing_types,
        limit=4,
    )
    open_questions: list[str] = []
    open_questions.extend(
        maybe_text(item)
        for item in evidence_adjudication.get("open_questions", [])
        if maybe_text(item)
    )
    open_questions.extend(
        maybe_text(item)
        for item in matching_adjudication.get("open_questions", [])
        if maybe_text(item)
    )
    for hypothesis_review in hypothesis_reviews:
        if not isinstance(hypothesis_review, dict):
            continue
        unresolved_legs = [
            maybe_text(item.get("leg_id"))
            for item in hypothesis_review.get("leg_reviews", [])
            if isinstance(item, dict) and maybe_text(item.get("status")) != "supported" and maybe_text(item.get("leg_id"))
        ]
        if unresolved_legs:
            open_questions.append(
                f"Does {maybe_text(hypothesis_review.get('hypothesis_id'))} need another round for these legs: {', '.join(unresolved_legs)}?"
            )
    open_questions.extend(
        f"How should the council resolve remand {maybe_text(item.get('remand_id'))}?"
        for item in state.get("remands_open", [])
        if isinstance(item, dict) and maybe_text(item.get("remand_id"))
    )
    matching_reasonable = bool(evidence_adjudication.get("matching_reasonable"))
    needs_additional_data = bool(evidence_adjudication.get("needs_additional_data")) or bool(remand_ids) or review_status != "supported"
    summary = (
        f"Investigation review is {review_status}. "
        f"Matched cards={len(matched_card_ids)}, isolated entries={len(isolated_entry_ids)}, remands={len(remand_ids)}. "
        f"{maybe_text(evidence_adjudication.get('summary')) or maybe_text(matching_result.get('summary'))}"
    ).strip()
    payload = {
        "schema_version": SCHEMA_VERSION,
        "review_id": f"investigation-review-{round_id}",
        "run_id": mission_run_id(mission),
        "round_id": round_id,
        "agent_role": "moderator",
        "authorization_id": maybe_text(matching_result.get("authorization_id"))
        or maybe_text(matching_adjudication.get("authorization_id"))
        or maybe_text(state.get("matching_authorization", {}).get("authorization_id")),
        "matching_result_id": maybe_text(matching_result.get("result_id")) or f"matching-result-{round_id}",
        "review_status": review_status,
        "matching_reasonable": matching_reasonable,
        "needs_additional_data": needs_additional_data,
        "summary": truncate_text(summary, 400),
        "hypothesis_reviews": hypothesis_reviews,
        "matched_card_ids": matched_card_ids,
        "isolated_entry_ids": isolated_entry_ids,
        "remand_ids": remand_ids,
        "open_questions": unique_strings(open_questions)[:6],
        "recommended_next_actions": recommendations,
    }
    validate_payload("investigation-review", payload)
    return payload


__all__ = [
    "INVESTIGATION_LEG_METRIC_FAMILIES",
    "build_hypothesis_review_from_state",
    "build_investigation_review_draft_from_state",
    "build_leg_review_from_state",
    "investigation_leg_metric_families",
    "investigation_review_overall_status",
    "observation_supports_investigation_leg",
    "record_has_investigation_tags",
    "record_matches_hypothesis_leg",
    "summarize_investigation_leg_status",
]
