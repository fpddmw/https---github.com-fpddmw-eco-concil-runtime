from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.investigation import build_investigation_plan  # noqa: E402
from eco_council_runtime.normalize import (  # noqa: E402
    build_evidence_cards_from_matches,
    build_matching_result,
    match_claims_to_observations,
    public_signals_to_claims,
)
from eco_council_runtime.reporting import build_hypothesis_review_from_state  # noqa: E402


def nyc_mission(*, hypotheses: list[str] | None = None) -> dict[str, object]:
    return {
        "run_id": "eco-20260327-nyc-smoke-canada-20230606",
        "topic": "New York City air quality discussion and Canadian wildfire smoke verification",
        "objective": "Verify whether wildfire smoke drove the New York City PM2.5 spike.",
        "window": {
            "start_utc": "2023-06-06T00:00:00Z",
            "end_utc": "2023-06-08T23:59:59Z",
        },
        "region": {
            "label": "New York City metro area, USA",
            "geometry": {
                "type": "BBox",
                "west": -74.30,
                "south": 40.45,
                "east": -73.65,
                "north": 40.95,
            },
        },
        "hypotheses": hypotheses
        or [
            "Canadian wildfire smoke was transported into the New York City metro area and drove the local PM2.5 spike."
        ],
    }


def nyc_observation() -> dict[str, object]:
    return {
        "observation_id": "obs-nyc-001",
        "source_skill": "openaq-data-fetch",
        "metric": "pm2_5",
        "aggregation": "point",
        "value": 42.0,
        "unit": "ug/m3",
        "time_window": {
            "start_utc": "2023-06-07T00:00:00Z",
            "end_utc": "2023-06-07T23:59:59Z",
        },
        "place_scope": {
            "label": "New York City metro area, USA",
            "geometry": {
                "type": "BBox",
                "west": -74.30,
                "south": 40.45,
                "east": -73.65,
                "north": 40.95,
            },
        },
        "quality_flags": [],
    }


class MatchingSemanticsTests(unittest.TestCase):
    def test_claim_without_signal_local_place_scope_stays_unmatched(self) -> None:
        mission = nyc_mission()
        signals = [
            {
                "signal_id": "pubsig-001",
                "source_skill": "bluesky-cascade-fetch",
                "signal_kind": "post",
                "title": "Smoke is awful today",
                "text": "The smoke is awful today and it smells like burning.",
                "artifact_path": "runs/example/bluesky.json",
                "record_locator": "$[0]",
                "published_at_utc": "2023-06-07T12:00:00Z",
            }
        ]

        claims = public_signals_to_claims(
            mission=mission,
            round_id="round-01",
            signals=signals,
            max_claims=3,
        )

        self.assertEqual(1, len(claims))
        claim = claims[0]
        self.assertFalse(claim["claim_scope"]["usable_for_matching"])
        self.assertEqual("mission-fallback", claim["claim_scope"]["place_source"])
        self.assertEqual("public_interpretation", claim["leg_id"])

        matches = match_claims_to_observations(
            claims=claims,
            observations=[nyc_observation()],
        )

        self.assertEqual([], matches[0]["observations"])
        self.assertIn(
            "Claim lacks signal-local place scope and cannot be treated as direct mission evidence yet.",
            matches[0]["gaps"],
        )

    def test_localized_public_claim_carries_hypothesis_and_leg_through_matching(self) -> None:
        mission = nyc_mission()
        signals = [
            {
                "signal_id": "pubsig-002",
                "source_skill": "gdelt-gkg-fetch",
                "signal_kind": "gkg-record",
                "title": "Smoke haze chokes New York City",
                "text": "New York City residents reported severe smoke and poor air quality during the haze episode.",
                "artifact_path": "runs/example/gkg.json",
                "record_locator": "$.downloads[0].gkg[0]",
                "published_at_utc": "2023-06-07T13:00:00Z",
                "raw_json": {
                    "locations": [
                        {
                            "name": "New York City",
                            "latitude": 40.7128,
                            "longitude": -74.0060,
                        }
                    ]
                },
            }
        ]

        claims = public_signals_to_claims(
            mission=mission,
            round_id="round-01",
            signals=signals,
            max_claims=3,
        )

        claim = claims[0]
        self.assertTrue(claim["claim_scope"]["usable_for_matching"])
        self.assertEqual("signal-derived", claim["claim_scope"]["place_source"])
        self.assertEqual("hypothesis-001", claim["hypothesis_id"])
        self.assertEqual("public_interpretation", claim["leg_id"])

        matches = match_claims_to_observations(
            claims=claims,
            observations=[nyc_observation()],
        )

        self.assertEqual("supports", matches[0]["verdict"])
        self.assertEqual(["obs-nyc-001"], [item["observation_id"] for item in matches[0]["observations"]])

        result = build_matching_result(
            authorization={
                "run_id": mission["run_id"],
                "round_id": "round-01",
                "authorization_id": "matchauth-round-01",
            },
            claims=claims,
            observations=[nyc_observation()],
            matches=matches,
        )
        pair = result["matched_pairs"][0]
        self.assertEqual("hypothesis-001", pair["hypothesis_id"])
        self.assertEqual("public_interpretation", pair["leg_id"])
        self.assertTrue(pair["matching_scope"]["usable_for_matching"])

        cards = build_evidence_cards_from_matches(matches)
        self.assertEqual("hypothesis-001", cards[0]["hypothesis_id"])
        self.assertEqual("public_interpretation", cards[0]["leg_id"])

    def test_investigation_review_filters_public_claims_by_hypothesis(self) -> None:
        mission = nyc_mission(
            hypotheses=[
                "Canadian wildfire smoke drove the New York City air-quality spike.",
                "Public concern in New York tracked the same smoke impact.",
            ]
        )
        plan = build_investigation_plan(mission=mission, round_id="round-01")
        state = {
            "cards_active": [
                {
                    "evidence_id": "evidence-002",
                    "claim_id": "claim-002",
                }
            ],
            "isolated_active": [],
            "remands_open": [],
            "observations": [],
            "claims": [
                {
                    "claim_id": "claim-001",
                    "claim_type": "smoke",
                    "hypothesis_id": "hypothesis-001",
                    "leg_id": "public_interpretation",
                },
                {
                    "claim_id": "claim-002",
                    "claim_type": "smoke",
                    "hypothesis_id": "hypothesis-002",
                    "leg_id": "public_interpretation",
                },
            ],
        }

        review_one = build_hypothesis_review_from_state(
            state=state,
            hypothesis=plan["hypotheses"][0],
            profile_id=plan["profile_id"],
        )
        review_two = build_hypothesis_review_from_state(
            state=state,
            hypothesis=plan["hypotheses"][1],
            profile_id=plan["profile_id"],
        )

        public_leg_one = {item["leg_id"]: item for item in review_one["leg_reviews"]}["public_interpretation"]
        public_leg_two = {item["leg_id"]: item for item in review_two["leg_reviews"]}["public_interpretation"]

        self.assertEqual([], public_leg_one["evidence_refs"])
        self.assertEqual(["card:evidence-002"], public_leg_two["evidence_refs"])


if __name__ == "__main__":
    unittest.main()
