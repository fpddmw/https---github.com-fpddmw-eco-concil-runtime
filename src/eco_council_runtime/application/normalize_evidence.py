"""Application services for normalize matching, evidence, and round snapshots."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Callable

from eco_council_runtime.adapters.filesystem import utc_now_iso
from eco_council_runtime.application.normalize_views import (
    compact_claim,
    compact_claim_submission,
    compact_evidence_card,
    compact_isolated_entry,
    compact_observation,
    compact_observation_submission,
    compact_remand_entry,
    compact_task,
    ordered_context_observations,
    representative_claim_submissions,
    representative_observation_submissions,
)
from eco_council_runtime.controller.paths import (
    claim_candidates_path,
    claim_curation_path,
    claim_submissions_path,
    data_readiness_report_path,
    evidence_adjudication_path,
    evidence_library_dir,
    matching_authorization_path,
    matching_result_path,
    observation_candidates_path,
    observation_curation_path,
    observation_submissions_path,
    round_dir,
    shared_claims_path,
    shared_evidence_path,
    shared_observations_path,
)
from eco_council_runtime.domain.normalize_semantics import (
    assess_observation_against_claim,
    build_evidence_summary,
    claim_matching_scope,
    direct_matching_gap_for_claim,
    geometry_overlap,
    metric_relevant,
    time_windows_overlap,
)
from eco_council_runtime.domain.text import maybe_text, unique_strings
from eco_council_runtime.investigation import causal_focus_for_role

IdEmitter = Callable[[str, int], str]
PayloadValidator = Callable[[str, Any], Any]


def _default_emit_row_id(prefix: str, index: int) -> str:
    return f"{prefix}-{index:03d}"


def _noop_validate_payload(_kind: str, _payload: Any) -> None:
    return None


def match_claims_to_observations(
    *,
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for claim in claims:
        matching_scope = claim_matching_scope(claim)
        support_score = 0
        contradict_score = 0
        primary_support_hits = 0
        contradict_hits = 0
        contextual_hits = 0
        notes: list[str] = []
        gaps: list[str] = []
        observation_assessments: list[dict[str, Any]] = []
        if matching_scope is None:
            matching: list[dict[str, Any]] = []
            scope_gap = direct_matching_gap_for_claim(claim)
            if scope_gap:
                gaps.append(scope_gap)
        else:
            notes.extend(
                maybe_text(item)
                for item in matching_scope.get("notes", [])
                if maybe_text(item)
            )
            matching = [
                observation
                for observation in observations
                if metric_relevant(maybe_text(claim.get("claim_type")), maybe_text(observation.get("metric")))
                and time_windows_overlap(matching_scope.get("time_window", {}), observation.get("time_window", {}))
                and geometry_overlap(
                    (matching_scope.get("place_scope") or {}).get("geometry", {}),
                    observation.get("place_scope", {}).get("geometry", {}),
                )
            ]
        for observation in matching:
            assessment = assess_observation_against_claim(
                maybe_text(claim.get("claim_type")),
                observation,
            )
            observation_assessments.append(
                {
                    "observation": observation,
                    "assessment": assessment,
                }
            )
            support_score += int(assessment.get("support_score") or 0)
            contradict_score += int(assessment.get("contradict_score") or 0)
            primary_support_hits += int(assessment.get("primary_support_hits") or 0)
            contradict_hits += int(assessment.get("contradict_hits") or 0)
            contextual_hits += int(assessment.get("contextual_hits") or 0)
            notes.extend(
                maybe_text(item)
                for item in assessment.get("notes", [])
                if maybe_text(item)
            )

        if not matching:
            verdict = "insufficient"
            confidence = "low"
            if matching_scope is not None:
                gaps.append("No observations matched the claim's localized window and geometry.")
        elif support_score > 0 and contradict_score == 0 and primary_support_hits > 0:
            verdict = "supports"
            confidence = "high" if support_score >= 4 and primary_support_hits >= 2 else "medium"
        elif support_score == 0 and contradict_score > 0:
            verdict = "contradicts"
            confidence = "high" if contradict_hits >= 2 else "medium"
        elif support_score > 0 and contradict_score > 0:
            verdict = "mixed"
            confidence = "medium"
        else:
            verdict = "insufficient"
            confidence = "low"
            if contextual_hits > 0 and primary_support_hits == 0 and contradict_hits == 0:
                gaps.append("Matched observations were contextual only and did not provide direct corroboration.")
            else:
                gaps.append("Matched observations did not cross the direct support or contradiction thresholds.")

        if matching_scope is not None and maybe_text(claim.get("claim_type")) in {"smoke", "air-pollution"}:
            if not any(item.get("source_skill") == "openaq-data-fetch" for item in matching):
                gaps.append("Station-grade corroboration is missing.")
            if any("modeled-background" in item.get("quality_flags", []) for item in matching):
                gaps.append("Modeled background fields should be cross-checked with station or local observations.")

        matches.append(
            {
                "claim": claim,
                "observations": matching,
                "support_score": support_score,
                "contradict_score": contradict_score,
                "observation_assessments": observation_assessments,
                "notes": notes,
                "gaps": sorted(dict.fromkeys(gaps)),
                "verdict": verdict,
                "confidence": confidence,
                "matching_scope": matching_scope,
            }
        )
    return matches


def build_matching_result(
    *,
    authorization: dict[str, Any],
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    matches: list[dict[str, Any]],
    schema_version: str = "1.0.0",
    validate_payload: PayloadValidator | None = None,
) -> dict[str, Any]:
    validate = validate_payload or _noop_validate_payload
    matched_pairs = [
        {
            "claim_id": maybe_text(match["claim"].get("claim_id")),
            "observation_ids": [
                maybe_text(item.get("observation_id"))
                for item in match["observations"]
                if maybe_text(item.get("observation_id"))
            ],
            "support_score": float(match["support_score"]),
            "contradict_score": float(match["contradict_score"]),
            "notes": [maybe_text(item) for item in match["notes"] if maybe_text(item)],
            "hypothesis_id": maybe_text(match["claim"].get("hypothesis_id")),
            "leg_id": maybe_text(match["claim"].get("leg_id")),
            "matching_scope": match.get("matching_scope"),
        }
        for match in matches
        if match["observations"]
    ]
    matched_claim_ids = [maybe_text(item["claim_id"]) for item in matched_pairs if maybe_text(item.get("claim_id"))]
    matched_observation_ids = unique_strings(
        [
            maybe_text(observation_id)
            for pair in matched_pairs
            for observation_id in pair.get("observation_ids", [])
            if maybe_text(observation_id)
        ]
    )
    all_claim_ids = [maybe_text(item.get("claim_id")) for item in claims if maybe_text(item.get("claim_id"))]
    all_observation_ids = [
        maybe_text(item.get("observation_id")) for item in observations if maybe_text(item.get("observation_id"))
    ]
    unmatched_claim_ids = [claim_id for claim_id in all_claim_ids if claim_id not in set(matched_claim_ids)]
    unmatched_observation_ids = [obs_id for obs_id in all_observation_ids if obs_id not in set(matched_observation_ids)]
    if matched_pairs and (unmatched_claim_ids or unmatched_observation_ids):
        result_status = "partial"
    elif matched_pairs:
        result_status = "matched"
    else:
        result_status = "unmatched"
    payload = {
        "schema_version": schema_version,
        "result_id": f"matchres-{maybe_text(authorization.get('round_id')) or 'round'}",
        "run_id": maybe_text(authorization.get("run_id")) or maybe_text(claims[0].get("run_id")) if claims else "",
        "round_id": maybe_text(authorization.get("round_id")) or maybe_text(claims[0].get("round_id")) if claims else "",
        "authorization_id": maybe_text(authorization.get("authorization_id")),
        "result_status": result_status,
        "summary": (
            f"Matched {len(matched_pairs)} claim-observation clusters, leaving "
            f"{len(unmatched_claim_ids)} unmatched claims and {len(unmatched_observation_ids)} unmatched observations."
        ),
        "matched_pairs": matched_pairs,
        "matched_claim_ids": matched_claim_ids,
        "matched_observation_ids": matched_observation_ids,
        "unmatched_claim_ids": unmatched_claim_ids,
        "unmatched_observation_ids": unmatched_observation_ids,
    }
    validate("matching-result", payload)
    return payload


def build_isolated_entries(
    *,
    run_id: str,
    round_id: str,
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    matches: list[dict[str, Any]],
    allow_isolated_evidence: bool,
    schema_version: str = "1.0.0",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not allow_isolated_evidence:
        return [], []
    matched_observation_ids = {
        maybe_text(observation.get("observation_id"))
        for match in matches
        for observation in match["observations"]
        if maybe_text(observation.get("observation_id"))
    }
    isolated: list[dict[str, Any]] = []
    for claim_index, match in enumerate(matches, start=1):
        claim = match["claim"]
        if match["observations"]:
            continue
        isolated.append(
            {
                "schema_version": schema_version,
                "isolated_id": f"isolated-claim-{claim_index:03d}",
                "run_id": run_id,
                "round_id": round_id,
                "entity_kind": "claim",
                "entity_id": maybe_text(claim.get("claim_id")),
                "summary": maybe_text(claim.get("summary")),
                "reason": "Public-side evidence is currently isolated from physical corroboration.",
            }
        )
    observation_index = 1
    for observation in observations:
        observation_id = maybe_text(observation.get("observation_id"))
        if not observation_id or observation_id in matched_observation_ids:
            continue
        isolated.append(
            {
                "schema_version": schema_version,
                "isolated_id": f"isolated-observation-{observation_index:03d}",
                "run_id": run_id,
                "round_id": round_id,
                "entity_kind": "observation",
                "entity_id": observation_id,
                "summary": f"{maybe_text(observation.get('metric'))} from {maybe_text(observation.get('source_skill'))}",
                "reason": "Physical-side evidence is currently isolated from attributable public recognition.",
            }
        )
        observation_index += 1
    return isolated, []


def build_remand_entries(
    *,
    run_id: str,
    round_id: str,
    matches: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    allow_isolated_evidence: bool,
    schema_version: str = "1.0.0",
) -> list[dict[str, Any]]:
    remands: list[dict[str, Any]] = []
    matched_observation_ids = {
        maybe_text(observation.get("observation_id"))
        for match in matches
        for observation in match["observations"]
        if maybe_text(observation.get("observation_id"))
    }
    for index, match in enumerate(matches, start=1):
        claim = match["claim"]
        claim_id = maybe_text(claim.get("claim_id"))
        if not claim_id:
            continue
        has_observations = bool(match["observations"])
        verdict = maybe_text(match["verdict"])
        if not has_observations and allow_isolated_evidence:
            continue
        if verdict not in {"mixed", "insufficient"} and has_observations:
            continue
        remands.append(
            {
                "schema_version": schema_version,
                "remand_id": f"remand-claim-{index:03d}",
                "run_id": run_id,
                "round_id": round_id,
                "entity_kind": "claim",
                "entity_id": claim_id,
                "summary": maybe_text(claim.get("summary")),
                "reasons": [maybe_text(item) for item in match["gaps"] if maybe_text(item)] or ["Matching remained partial."],
            }
        )
    if allow_isolated_evidence:
        return remands
    observation_index = 1
    for observation in observations:
        observation_id = maybe_text(observation.get("observation_id"))
        if not observation_id or observation_id in matched_observation_ids:
            continue
        remands.append(
            {
                "schema_version": schema_version,
                "remand_id": f"remand-observation-{observation_index:03d}",
                "run_id": run_id,
                "round_id": round_id,
                "entity_kind": "observation",
                "entity_id": observation_id,
                "summary": f"{maybe_text(observation.get('metric'))} from {maybe_text(observation.get('source_skill'))}",
                "reasons": ["Observation remained unmatched and isolated evidence was not authorized."],
            }
        )
        observation_index += 1
    return remands


def build_evidence_adjudication(
    *,
    authorization: dict[str, Any],
    matching_result: dict[str, Any],
    evidence_cards: list[dict[str, Any]],
    isolated_entries: list[dict[str, Any]],
    remands: list[dict[str, Any]],
    schema_version: str = "1.0.0",
    validate_payload: PayloadValidator | None = None,
) -> dict[str, Any]:
    validate = validate_payload or _noop_validate_payload
    if remands and evidence_cards:
        status = "partial"
    elif remands:
        status = "remand-required"
    else:
        status = "complete"
    payload = {
        "schema_version": schema_version,
        "adjudication_id": f"adjudication-{maybe_text(authorization.get('round_id')) or 'round'}",
        "run_id": maybe_text(authorization.get("run_id")),
        "round_id": maybe_text(authorization.get("round_id")),
        "authorization_id": maybe_text(authorization.get("authorization_id")),
        "matching_result_id": maybe_text(matching_result.get("result_id")),
        "adjudication_status": status,
        "summary": (
            f"Produced {len(evidence_cards)} evidence cards, {len(isolated_entries)} isolated entries, "
            f"and {len(remands)} open remands."
        ),
        "matching_reasonable": bool(evidence_cards or isolated_entries or remands),
        "needs_additional_data": bool(remands),
        "card_ids": [maybe_text(item.get("evidence_id")) for item in evidence_cards if maybe_text(item.get("evidence_id"))],
        "isolated_entry_ids": [
            maybe_text(item.get("isolated_id"))
            for item in isolated_entries
            if maybe_text(item.get("isolated_id"))
        ],
        "remand_ids": [maybe_text(item.get("remand_id")) for item in remands if maybe_text(item.get("remand_id"))],
        "open_questions": unique_strings(
            [
                f"How should the council resolve remand {maybe_text(item.get('remand_id'))}?"
                for item in remands
                if maybe_text(item.get("remand_id"))
            ]
        ),
        "recommended_next_actions": [],
    }
    validate("evidence-adjudication", payload)
    return payload


def build_evidence_cards_from_matches(
    matches: list[dict[str, Any]],
    *,
    schema_version: str = "1.0.0",
    emit_row_id: IdEmitter | None = None,
    validate_payload: PayloadValidator | None = None,
) -> list[dict[str, Any]]:
    emit = emit_row_id or _default_emit_row_id
    validate = validate_payload or _noop_validate_payload
    evidence_cards: list[dict[str, Any]] = []
    for index, match in enumerate(matches, start=1):
        if not isinstance(match.get("observations"), list) or not match.get("observations"):
            continue
        claim = match["claim"]
        evidence = {
            "schema_version": schema_version,
            "evidence_id": emit("evidence", index),
            "run_id": claim["run_id"],
            "round_id": claim["round_id"],
            "claim_id": claim["claim_id"],
            "verdict": match["verdict"],
            "confidence": match["confidence"],
            "summary": build_evidence_summary(claim, match["notes"], match["verdict"], match["gaps"]),
            "public_refs": claim.get("public_refs", []),
            "observation_ids": [item["observation_id"] for item in match["observations"]],
            "gaps": match["gaps"],
        }
        hypothesis_id = maybe_text(claim.get("hypothesis_id"))
        if hypothesis_id:
            evidence["hypothesis_id"] = hypothesis_id
        leg_id = maybe_text(claim.get("leg_id"))
        if leg_id:
            evidence["leg_id"] = leg_id
        if isinstance(match.get("matching_scope"), dict):
            evidence["matching_scope"] = match.get("matching_scope")
        validate("evidence-card", evidence)
        evidence_cards.append(evidence)
    return evidence_cards


def link_claims_to_evidence(
    *,
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    schema_version: str = "1.0.0",
    emit_row_id: IdEmitter | None = None,
    validate_payload: PayloadValidator | None = None,
) -> list[dict[str, Any]]:
    matches = match_claims_to_observations(claims=claims, observations=observations)
    return build_evidence_cards_from_matches(
        matches,
        schema_version=schema_version,
        emit_row_id=emit_row_id,
        validate_payload=validate_payload,
    )


def build_round_snapshot(
    *,
    run_dir: Path,
    round_id: str,
    run: dict[str, Any],
    tasks: list[dict[str, Any]],
    claims: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    evidence_cards: list[dict[str, Any]],
    role: str,
    state: dict[str, Any],
    investigation_plan: dict[str, Any] | None = None,
    matching_authorization: dict[str, Any] | None = None,
    generated_at_utc: str | None = None,
    max_context_tasks: int = 4,
    max_context_claims: int = 4,
    max_context_observations: int = 8,
    max_context_evidence: int = 4,
) -> dict[str, Any]:
    claim_candidates_current = (
        state.get("claim_candidates_current", [])
        if isinstance(state.get("claim_candidates_current"), list)
        else []
    )
    observation_candidates_current = (
        state.get("observation_candidates_current", [])
        if isinstance(state.get("observation_candidates_current"), list)
        else []
    )
    claim_curation = state.get("claim_curation", {}) if isinstance(state.get("claim_curation"), dict) else {}
    observation_curation = (
        state.get("observation_curation", {}) if isinstance(state.get("observation_curation"), dict) else {}
    )
    matching_authorization_obj = (
        matching_authorization if isinstance(matching_authorization, dict) else {}
    )
    role_tasks = [task for task in tasks if role == "moderator" or task.get("assigned_role") == role]
    verdict_counter = Counter(maybe_text(item.get("verdict")) for item in evidence_cards)
    focus_claims = claims
    if role == "environmentalist":
        focus_claims = [claim for claim in claims if claim.get("needs_physical_validation")]

    dataset = {
        "generated_at_utc": generated_at_utc or utc_now_iso(),
        "task_count": len(role_tasks),
        "claim_count": len(claims),
        "observation_count": len(observations),
        "evidence_count": len(evidence_cards),
        "claim_submission_count": len(state.get("claim_submissions_auditable", [])),
        "observation_submission_count": len(state.get("observation_submissions_auditable", [])),
        "claim_submission_current_count": len(state.get("claim_submissions_current", [])),
        "observation_submission_current_count": len(state.get("observation_submissions_current", [])),
        "claims_active_count": len(state.get("claims_active", [])),
        "observations_active_count": len(state.get("observations_active", [])),
        "claim_candidate_count": len(claim_candidates_current),
        "observation_candidate_count": len(observation_candidates_current),
        "cards_active_count": len(state.get("cards_active", [])),
        "isolated_count": len(state.get("isolated_active", [])),
        "remand_count": len(state.get("remands_open", [])),
    }
    focus = {
        "task_ids": [maybe_text(task.get("task_id")) for task in role_tasks],
        "claims_needing_more_evidence": [
            card["claim_id"] for card in evidence_cards if card.get("verdict") in {"mixed", "insufficient"}
        ],
    }
    if role == "sociologist":
        focus["candidate_claim_ids"] = [
            maybe_text(item.get("claim_id"))
            for item in (focus_claims or claim_candidates_current)
            if maybe_text(item.get("claim_id"))
        ]
    if role == "environmentalist":
        focus["metrics_requested"] = sorted(
            {
                maybe_text(observation.get("metric"))
                for observation in (observations or observation_candidates_current)
                if maybe_text(observation.get("metric"))
            }
        )

    compact_claims_list = [compact_claim(item) for item in focus_claims[:max_context_claims]]
    compact_evidence = [compact_evidence_card(item) for item in evidence_cards[:max_context_evidence]]
    compact_observations = [
        compact_observation(item)
        for item in ordered_context_observations(observations, evidence_cards, claims=claims)[:max_context_observations]
    ]
    auditable_claim_submissions = representative_claim_submissions(state.get("claim_submissions_auditable", []))
    auditable_observation_submissions = representative_observation_submissions(
        state.get("observation_submissions_auditable", []),
        claims,
    )
    current_claim_submissions = representative_claim_submissions(state.get("claim_submissions_current", []))
    current_observation_submissions = representative_observation_submissions(
        state.get("observation_submissions_current", []),
        claims,
    )

    return {
        "context_layer": "evidence-library-v1",
        "run": run,
        "dataset": dataset,
        "causal_focus": causal_focus_for_role(investigation_plan, role) if isinstance(investigation_plan, dict) else {},
        "phase_state": {
            "claim_curation_status": maybe_text(claim_curation.get("status")),
            "observation_curation_status": maybe_text(observation_curation.get("status")),
            "readiness_statuses": {
                report_role: maybe_text(report.get("readiness_status"))
                for report_role, report in state.get("readiness_reports", {}).items()
                if isinstance(report, dict)
            },
            "matching_authorization_status": maybe_text(matching_authorization_obj.get("authorization_status")),
            "matching_authorization_basis": maybe_text(matching_authorization_obj.get("authorization_basis")),
            "matching_result_status": maybe_text((state.get("matching_result") or {}).get("result_status")),
            "adjudication_status": maybe_text((state.get("evidence_adjudication") or {}).get("adjudication_status")),
        },
        "aggregates": {
            "claim_type_counts": dict(Counter(maybe_text(item.get("claim_type")) for item in claims)),
            "observation_metric_counts": dict(Counter(maybe_text(item.get("metric")) for item in observations)),
            "evidence_verdict_counts": dict(verdict_counter),
        },
        "canonical_paths": {
            "tasks": str(round_dir(run_dir, round_id) / "moderator" / "tasks.json"),
            "claims": str(shared_claims_path(run_dir, round_id)),
            "observations": str(shared_observations_path(run_dir, round_id)),
            "evidence_cards": str(shared_evidence_path(run_dir, round_id)),
            "claim_submissions": str(claim_submissions_path(run_dir, round_id)),
            "observation_submissions": str(observation_submissions_path(run_dir, round_id)),
            "claim_candidates": str(claim_candidates_path(run_dir, round_id)),
            "observation_candidates": str(observation_candidates_path(run_dir, round_id)),
            "claim_curation": str(claim_curation_path(run_dir, round_id)),
            "observation_curation": str(observation_curation_path(run_dir, round_id)),
            "sociologist_data_readiness_report": str(data_readiness_report_path(run_dir, round_id, "sociologist")),
            "environmentalist_data_readiness_report": str(
                data_readiness_report_path(run_dir, round_id, "environmentalist")
            ),
            "matching_authorization": str(matching_authorization_path(run_dir, round_id)),
            "matching_result": str(matching_result_path(run_dir, round_id)),
            "evidence_adjudication": str(evidence_adjudication_path(run_dir, round_id)),
            "evidence_library_dir": str(evidence_library_dir(run_dir, round_id)),
        },
        "tasks": [compact_task(item) for item in role_tasks[:max_context_tasks]],
        "focus": focus,
        "claims": compact_claims_list,
        "observations": compact_observations,
        "evidence_cards": compact_evidence,
        "evidence_library": {
            "claim_submissions_auditable": [
                compact_claim_submission(item) for item in auditable_claim_submissions[:max_context_claims]
            ],
            "observation_submissions_auditable": [
                compact_observation_submission(item)
                for item in auditable_observation_submissions[:max_context_observations]
            ],
            "claim_submissions_current": [
                compact_claim_submission(item) for item in current_claim_submissions[:max_context_claims]
            ],
            "observation_submissions_current": [
                compact_observation_submission(item)
                for item in current_observation_submissions[:max_context_observations]
            ],
            "claim_candidates_current": [compact_claim(item) for item in claim_candidates_current[:max_context_claims]],
            "observation_candidates_current": [
                compact_observation(item) for item in observation_candidates_current[:max_context_observations]
            ],
            "claim_curation": {
                "status": maybe_text(claim_curation.get("status")),
                "curated_claim_count": len(claim_curation.get("curated_claims", []))
                if isinstance(claim_curation.get("curated_claims"), list)
                else 0,
            },
            "observation_curation": {
                "status": maybe_text(observation_curation.get("status")),
                "curated_observation_count": len(observation_curation.get("curated_observations", []))
                if isinstance(observation_curation.get("curated_observations"), list)
                else 0,
            },
            "claims_active": [
                compact_claim_submission(item)
                for item in (state.get("claims_active", [])[:max_context_claims])
            ],
            "observations_active": [
                compact_observation_submission(item)
                for item in (state.get("observations_active", [])[:max_context_observations])
            ],
            "cards_active": [
                compact_evidence_card(item) for item in (state.get("cards_active", [])[:max_context_evidence])
            ],
            "isolated_active": [
                compact_isolated_entry(item) for item in (state.get("isolated_active", [])[:max_context_evidence])
            ],
            "remands_open": [
                compact_remand_entry(item) for item in (state.get("remands_open", [])[:max_context_evidence])
            ],
        },
    }


__all__ = [
    "build_evidence_adjudication",
    "build_evidence_cards_from_matches",
    "build_isolated_entries",
    "build_matching_result",
    "build_remand_entries",
    "build_round_snapshot",
    "link_claims_to_evidence",
    "match_claims_to_observations",
]
