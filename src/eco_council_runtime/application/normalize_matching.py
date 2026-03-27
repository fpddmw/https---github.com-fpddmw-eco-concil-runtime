"""Application builders for normalize matching preparation outputs."""

from __future__ import annotations

from typing import Any, Callable

from eco_council_runtime.domain.text import maybe_text


CompactPayloadBuilder = Callable[[dict[str, Any]], dict[str, Any]]
PayloadValidator = Callable[[str, dict[str, Any]], Any]


def build_matching_candidate_set(
    *,
    authorization: dict[str, Any],
    matches: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    schema_version: str,
    compact_claim: CompactPayloadBuilder,
    compact_observation: CompactPayloadBuilder,
) -> dict[str, Any]:
    matched_observation_ids = {
        maybe_text(observation.get("observation_id"))
        for match in matches
        for observation in match.get("observations", [])
        if isinstance(observation, dict) and maybe_text(observation.get("observation_id"))
    }
    claim_candidates: list[dict[str, Any]] = []
    for match in matches:
        claim = match.get("claim", {})
        observation_candidates: list[dict[str, Any]] = []
        for candidate in match.get("observation_assessments", []):
            observation = candidate.get("observation", {}) if isinstance(candidate, dict) else {}
            assessment = candidate.get("assessment", {}) if isinstance(candidate, dict) else {}
            observation_candidates.append(
                {
                    "observation": compact_observation(observation if isinstance(observation, dict) else {}),
                    "assessment": {
                        "support_score": int(assessment.get("support_score") or 0),
                        "contradict_score": int(assessment.get("contradict_score") or 0),
                        "primary_support_hits": int(assessment.get("primary_support_hits") or 0),
                        "contradict_hits": int(assessment.get("contradict_hits") or 0),
                        "contextual_hits": int(assessment.get("contextual_hits") or 0),
                    },
                    "notes": [maybe_text(item) for item in assessment.get("notes", []) if maybe_text(item)][:6],
                }
            )
        claim_candidates.append(
            {
                "claim": compact_claim(claim if isinstance(claim, dict) else {}),
                "suggested_verdict": maybe_text(match.get("verdict")),
                "suggested_confidence": maybe_text(match.get("confidence")),
                "support_score": int(match.get("support_score") or 0),
                "contradict_score": int(match.get("contradict_score") or 0),
                "matched_observation_ids": [
                    maybe_text(item.get("observation_id"))
                    for item in match.get("observations", [])
                    if isinstance(item, dict) and maybe_text(item.get("observation_id"))
                ],
                "matching_scope": match.get("matching_scope"),
                "gaps": [maybe_text(item) for item in match.get("gaps", []) if maybe_text(item)][:6],
                "notes": [maybe_text(item) for item in match.get("notes", []) if maybe_text(item)][:8],
                "observation_candidates": observation_candidates[:12],
            }
        )
    unpaired_observations = [
        compact_observation(observation)
        for observation in observations
        if isinstance(observation, dict)
        and maybe_text(observation.get("observation_id"))
        and maybe_text(observation.get("observation_id")) not in matched_observation_ids
    ]
    return {
        "schema_version": schema_version,
        "candidate_set_id": f"matchcand-{maybe_text(authorization.get('round_id')) or 'round'}",
        "run_id": maybe_text(authorization.get("run_id")),
        "round_id": maybe_text(authorization.get("round_id")),
        "authorization_id": maybe_text(authorization.get("authorization_id")),
        "summary": (
            f"Rule nomination produced {len(claim_candidates)} claim-side candidate clusters and "
            f"{len(unpaired_observations)} currently unpaired observations."
        ),
        "claim_candidates": claim_candidates,
        "unpaired_observation_candidates": unpaired_observations[:24],
    }


def build_matching_adjudication_draft(
    *,
    authorization: dict[str, Any],
    candidate_set: dict[str, Any],
    matching_result: dict[str, Any],
    evidence_cards: list[dict[str, Any]],
    isolated_entries: list[dict[str, Any]],
    remands: list[dict[str, Any]],
    evidence_adjudication: dict[str, Any],
    schema_version: str,
    validate_payload: PayloadValidator,
) -> dict[str, Any]:
    payload = {
        "schema_version": schema_version,
        "adjudication_id": maybe_text(evidence_adjudication.get("adjudication_id")) or f"adjudication-{maybe_text(authorization.get('round_id')) or 'round'}",
        "run_id": maybe_text(authorization.get("run_id")),
        "round_id": maybe_text(authorization.get("round_id")),
        "agent_role": "moderator",
        "authorization_id": maybe_text(authorization.get("authorization_id")),
        "candidate_set_id": maybe_text(candidate_set.get("candidate_set_id")),
        "summary": (
            f"Rule draft proposes {len(evidence_cards)} evidence cards, {len(isolated_entries)} isolated entries, "
            f"and {len(remands)} remands for moderator review."
        ),
        "rationale": (
            "This draft is rule-nominated only. The moderator should merge, prune, or reclassify matches "
            "based on cross-source coherence, representativeness, and whether isolated evidence remains acceptable."
        ),
        "matching_result": matching_result,
        "evidence_cards": evidence_cards,
        "isolated_entries": isolated_entries,
        "remand_entries": remands,
        "evidence_adjudication": evidence_adjudication,
        "open_questions": [
            maybe_text(item)
            for item in evidence_adjudication.get("open_questions", [])
            if maybe_text(item)
        ],
        "recommended_next_actions": [],
    }
    validate_payload("matching-adjudication", payload)
    return payload


__all__ = [
    "build_matching_adjudication_draft",
    "build_matching_candidate_set",
]
