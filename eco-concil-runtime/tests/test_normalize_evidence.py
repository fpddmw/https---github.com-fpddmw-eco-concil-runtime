from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.application.normalize_evidence import (  # noqa: E402
    build_evidence_adjudication,
    build_evidence_cards_from_matches,
    build_isolated_entries,
    build_matching_result,
    build_remand_entries,
    build_round_snapshot,
    link_claims_to_evidence,
    match_claims_to_observations,
)
from eco_council_runtime.application.normalize_library import (  # noqa: E402
    claim_submission_from_claim,
    observation_submission_from_observation,
)
from eco_council_runtime.investigation import build_investigation_plan  # noqa: E402


def bbox_scope() -> dict[str, object]:
    return {
        "label": "New York City metro area, USA",
        "geometry": {
            "type": "BBox",
            "west": -74.30,
            "south": 40.45,
            "east": -73.65,
            "north": 40.95,
        },
    }


def mission_window() -> dict[str, str]:
    return {
        "start_utc": "2023-06-06T00:00:00Z",
        "end_utc": "2023-06-08T23:59:59Z",
    }


def smoke_claim() -> dict[str, object]:
    scope = bbox_scope()
    window = mission_window()
    return {
        "claim_id": "claim-001",
        "run_id": "run-001",
        "round_id": "round-001",
        "claim_type": "smoke",
        "summary": "Smoke impacted New York City",
        "statement": "Canadian wildfire smoke impacted New York City.",
        "priority": 2,
        "needs_physical_validation": True,
        "public_refs": [{"source_skill": "youtube-video-search", "external_id": "vid-001"}],
        "hypothesis_id": "hypothesis-001",
        "leg_id": "public_interpretation",
        "time_window": window,
        "place_scope": scope,
        "claim_scope": {
            "time_window": window,
            "place_scope": scope,
            "time_source": "signal-derived",
            "place_source": "signal-derived",
            "usable_for_matching": True,
            "notes": [],
        },
    }


def pm25_observation() -> dict[str, object]:
    return {
        "observation_id": "obs-001",
        "run_id": "run-001",
        "round_id": "round-001",
        "source_skill": "openaq-data-fetch",
        "metric": "pm2_5",
        "aggregation": "point",
        "value": 42.0,
        "unit": "ug/m3",
        "time_window": mission_window(),
        "place_scope": bbox_scope(),
        "quality_flags": [],
        "hypothesis_id": "hypothesis-001",
        "leg_id": "impact",
    }


class NormalizeEvidenceTests(unittest.TestCase):
    def test_matching_and_evidence_builders_are_directly_importable(self) -> None:
        claims = [smoke_claim()]
        observations = [pm25_observation()]

        matches = match_claims_to_observations(claims=claims, observations=observations)
        self.assertEqual(1, len(matches))
        self.assertEqual("supports", matches[0]["verdict"])
        self.assertEqual(["obs-001"], [item["observation_id"] for item in matches[0]["observations"]])

        validation_calls: list[tuple[str, str]] = []
        result = build_matching_result(
            authorization={
                "run_id": "run-001",
                "round_id": "round-001",
                "authorization_id": "auth-001",
            },
            claims=claims,
            observations=observations,
            matches=matches,
            validate_payload=lambda kind, payload: validation_calls.append((kind, payload["result_id"])),  # noqa: E731
        )
        self.assertEqual("matched", result["result_status"])
        self.assertEqual([("matching-result", "matchres-round-001")], validation_calls)

        validation_calls.clear()
        cards = build_evidence_cards_from_matches(
            matches,
            validate_payload=lambda kind, payload: validation_calls.append((kind, payload["evidence_id"])),  # noqa: E731
        )
        self.assertEqual(1, len(cards))
        self.assertEqual("hypothesis-001", cards[0]["hypothesis_id"])
        self.assertEqual("public_interpretation", cards[0]["leg_id"])
        self.assertEqual([("evidence-card", "evidence-001")], validation_calls)

        linked_cards = link_claims_to_evidence(claims=claims, observations=observations)
        self.assertEqual(1, len(linked_cards))
        self.assertEqual(cards[0]["claim_id"], linked_cards[0]["claim_id"])

        unmatched_matches = match_claims_to_observations(claims=claims, observations=[])
        isolated_entries, _ = build_isolated_entries(
            run_id="run-001",
            round_id="round-001",
            claims=claims,
            observations=[],
            matches=unmatched_matches,
            allow_isolated_evidence=True,
        )
        remands = build_remand_entries(
            run_id="run-001",
            round_id="round-001",
            matches=unmatched_matches,
            observations=[],
            allow_isolated_evidence=False,
        )
        self.assertEqual(1, len(isolated_entries))
        self.assertEqual(1, len(remands))

        validation_calls.clear()
        adjudication = build_evidence_adjudication(
            authorization={
                "run_id": "run-001",
                "round_id": "round-001",
                "authorization_id": "auth-001",
            },
            matching_result=result,
            evidence_cards=cards,
            isolated_entries=isolated_entries,
            remands=remands,
            validate_payload=lambda kind, payload: validation_calls.append((kind, payload["adjudication_id"])),  # noqa: E731
        )
        self.assertEqual("partial", adjudication["adjudication_status"])
        self.assertEqual([("evidence-adjudication", "adjudication-round-001")], validation_calls)

    def test_round_snapshot_builder_is_directly_importable(self) -> None:
        mission = {
            "run_id": "run-001",
            "topic": "New York City smoke verification",
            "objective": "Verify whether smoke drove the PM2.5 spike.",
            "window": mission_window(),
            "region": bbox_scope(),
            "hypotheses": [
                "Canadian wildfire smoke was transported into the New York City metro area and drove the local PM2.5 spike."
            ],
        }
        claim = smoke_claim()
        observation = pm25_observation()
        evidence_cards = [
            {
                "evidence_id": "evidence-001",
                "claim_id": "claim-001",
                "verdict": "supports",
                "confidence": "high",
                "summary": "PM2.5 evidence supports the smoke claim.",
                "observation_ids": ["obs-001"],
                "gaps": [],
            }
        ]
        tasks = [
            {
                "task_id": "task-001",
                "assigned_role": "environmentalist",
                "objective": "Verify PM2.5 spike",
                "status": "open",
                "inputs": {
                    "evidence_requirements": [
                        {"requirement_type": "physical"},
                    ]
                },
            }
        ]
        state = {
            "claim_candidates_current": [claim],
            "observation_candidates_current": [observation],
            "claim_curation": {"status": "completed", "curated_claims": [claim]},
            "observation_curation": {"status": "completed", "curated_observations": [observation]},
            "claim_submissions_current": [claim_submission_from_claim(claim)],
            "observation_submissions_current": [observation_submission_from_observation(observation)],
            "claim_submissions_auditable": [claim_submission_from_claim(claim)],
            "observation_submissions_auditable": [observation_submission_from_observation(observation)],
            "claims_active": [claim_submission_from_claim(claim)],
            "observations_active": [observation_submission_from_observation(observation)],
            "cards_active": evidence_cards,
            "isolated_active": [],
            "remands_open": [],
            "matching_result": {"result_status": "matched"},
            "evidence_adjudication": {"adjudication_status": "complete"},
            "readiness_reports": {
                "sociologist": {"readiness_status": "ready"},
                "environmentalist": {"readiness_status": "ready"},
            },
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            run_dir = Path(temp_dir)
            snapshot = build_round_snapshot(
                run_dir=run_dir,
                round_id="round-001",
                run={
                    "run_id": "run-001",
                    "round_id": "round-001",
                    "topic": mission["topic"],
                    "objective": mission["objective"],
                    "region": mission["region"],
                    "window": mission["window"],
                    "role": "environmentalist",
                },
                tasks=tasks,
                claims=[claim],
                observations=[observation],
                evidence_cards=evidence_cards,
                role="environmentalist",
                state=state,
                investigation_plan=build_investigation_plan(mission=mission, round_id="round-001"),
                matching_authorization={
                    "authorization_status": "authorized",
                    "authorization_basis": "moderator-approved",
                },
            )

        self.assertEqual("evidence-library-v1", snapshot["context_layer"])
        self.assertEqual(1, snapshot["dataset"]["claim_count"])
        self.assertEqual(1, snapshot["dataset"]["observation_count"])
        self.assertEqual(["pm2_5"], snapshot["focus"]["metrics_requested"])
        self.assertEqual("completed", snapshot["phase_state"]["claim_curation_status"])
        self.assertEqual("authorized", snapshot["phase_state"]["matching_authorization_status"])
        self.assertEqual("matched", snapshot["phase_state"]["matching_result_status"])
        self.assertEqual("complete", snapshot["phase_state"]["adjudication_status"])
        self.assertEqual(1, len(snapshot["claims"]))
        self.assertEqual(1, len(snapshot["observations"]))
        self.assertEqual(1, len(snapshot["evidence_cards"]))
        self.assertTrue(snapshot["canonical_paths"]["evidence_cards"].endswith("shared/evidence_cards.json"))


if __name__ == "__main__":
    unittest.main()
