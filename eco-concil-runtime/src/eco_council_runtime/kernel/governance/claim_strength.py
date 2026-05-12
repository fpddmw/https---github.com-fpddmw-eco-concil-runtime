from __future__ import annotations

from typing import Any

CLAIM_STRENGTH_OBLIGATIONS_SCHEMA_VERSION = "claim-strength-obligations-v1"
CLAIM_STRENGTH_CLOSING_ITEM_ID = "review-claim-strength-and-report-boundary"


def claim_strength_obligations() -> dict[str, Any]:
    return {
        "schema_version": CLAIM_STRENGTH_OBLIGATIONS_SCHEMA_VERSION,
        "semantics": (
            "Thin governance surface only. It asks the council to make report "
            "boundary and claim strength explicit; it does not classify topics, "
            "evaluate evidence quality, rank sources, or choose an agenda."
        ),
        "report_boundary": {
            "weak_or_bounded_report": (
                "Allowed when the moderator explicitly records limitations, "
                "unresolved refs, and why live actionable investigation routes "
                "are not being continued in the current run."
            ),
            "not_allowed_as_shortcut": (
                "A weak report must not convert missing evidence, failed fetches, "
                "or zero-signal attempts into proof that no further source path "
                "exists."
            ),
            "strong_report": (
                "A report asserting relational, causal, transport, origin, or "
                "source-attribution claims should cite explicit hypothesis, "
                "finding, evidence-bundle, signal, or receipt refs and expose a "
                "challenger review path."
            ),
        },
        "claim_strengths": [
            {
                "strength": "descriptive",
                "meaning": (
                    "Describes observed records, reports, measurements, or "
                    "agent findings without asserting a causal or source chain."
                ),
                "expected_record": (
                    "Cite the visible records or explain that the statement is "
                    "a limitation-aware summary."
                ),
            },
            {
                "strength": "relational",
                "meaning": (
                    "Links two or more observed facts, timelines, places, actors, "
                    "or datasets without asserting a full causal chain."
                ),
                "expected_record": (
                    "Cite the linked refs and record unresolved alternatives or "
                    "continuation paths."
                ),
            },
            {
                "strength": "causal_or_source_attribution",
                "meaning": (
                    "Attributes an effect, transport path, origin, responsibility, "
                    "or source chain."
                ),
                "expected_record": (
                    "Cite explicit supporting refs and make challenger review or "
                    "continuation visible before final report closure."
                ),
            },
        ],
        "moderator_obligations": [
            "Record round synthesis before closing a round with unresolved refs.",
            "Record whether the report is descriptive, relational, or causal/source-attribution.",
            "Carry actionable unresolved refs into continuation, or record why they are not continued.",
            "Preserve investigator freedom to propose sources, revise queries, and combine evidence.",
        ],
        "non_goals": [
            "No topic-specific source requirements.",
            "No source ordering, weighting, or evidence scoring.",
            "No automatic downgrade of the user's requested report boundary.",
            "No fixed investigation agenda.",
        ],
    }


def claim_strength_closing_item(
    *,
    unresolved_refs: list[str],
    source_attempt_review_refs: list[str],
) -> dict[str, Any]:
    review_required = bool(unresolved_refs or source_attempt_review_refs)
    return {
        "item_id": CLAIM_STRENGTH_CLOSING_ITEM_ID,
        "state": "review-required" if review_required else "available",
        "unresolved_ref_count": len(unresolved_refs),
        "source_attempt_review_ref_count": len(source_attempt_review_refs),
        "unresolved_refs": unresolved_refs[:20],
        "source_attempt_review_refs": source_attempt_review_refs[:20],
        "weak_report_allowed": True,
        "required_moderator_record": (
            "Before issuing a weak or bounded report, record the selected claim "
            "strength, the unresolved refs or source-attempt limitations, and "
            "why continuation is not being opened now. If actionable follow-up "
            "remains live, open or request a continuation round instead of "
            "prematurely treating the report as complete."
        ),
        "strong_claim_boundary": (
            "If the report makes relational, causal, transport, origin, or "
            "source-attribution claims, cite explicit hypothesis/finding/"
            "evidence-bundle/signal/receipt refs and expose challenger review "
            "or continuation. Runtime does not judge whether those refs are "
            "sufficient; the council must deliberate that."
        ),
        "challenger_surface": (
            "Challenger can open or dispose challenge tickets against strong "
            "claims, unresolved alternatives, or premature closure."
        ),
        "reflection_surfaces": [
            "submit-round-synthesis",
            "submit-readiness-opinion",
            "submit-agent-position",
            "submit-evidence-request",
            "submit-source-acquisition-proposal",
            "update-hypothesis-status",
            "open-challenge-ticket",
            "submit-challenge-disposition",
        ],
    }


__all__ = [
    "CLAIM_STRENGTH_CLOSING_ITEM_ID",
    "CLAIM_STRENGTH_OBLIGATIONS_SCHEMA_VERSION",
    "claim_strength_closing_item",
    "claim_strength_obligations",
]
