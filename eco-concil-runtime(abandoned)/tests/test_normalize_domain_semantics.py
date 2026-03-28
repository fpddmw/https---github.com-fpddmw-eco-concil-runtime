from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.domain.normalize_semantics import (  # noqa: E402
    assess_observation_against_claim,
    build_evidence_summary,
    build_public_claim_scope,
    claim_matching_scope,
    claim_priority_metric_families,
    direct_matching_gap_for_claim,
    infer_observation_investigation_tags,
    observation_metric_family,
)
from eco_council_runtime.investigation import build_investigation_plan  # noqa: E402


def smoke_mission() -> dict[str, object]:
    return {
        "run_id": "eco-20260327-nyc-smoke-canada-20230606",
        "topic": "New York City air quality discussion and Canadian wildfire smoke verification",
        "objective": "Verify whether Canadian wildfire smoke drove the New York City PM2.5 spike.",
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
        "hypotheses": [
            "Canadian wildfire smoke was transported into the New York City metro area and drove the local PM2.5 spike."
        ],
    }


def point_scope(label: str, latitude: float, longitude: float) -> dict[str, object]:
    return {
        "label": label,
        "geometry": {
            "type": "Point",
            "latitude": latitude,
            "longitude": longitude,
        },
    }


class NormalizeDomainSemanticsTests(unittest.TestCase):
    def test_public_claim_scope_is_directly_importable_and_preserves_matching_rules(self) -> None:
        mission = smoke_mission()
        scope = build_public_claim_scope(
            signals=[
                {
                    "published_at_utc": "2023-06-07T13:00:00Z",
                    "text": "Smoke is awful today but nobody says where.",
                }
            ],
            mission_scope=mission["region"],
            mission_time_window=mission["window"],
        )

        self.assertEqual("signal-derived", scope["time_source"])
        self.assertEqual("mission-fallback", scope["place_source"])
        self.assertFalse(scope["usable_for_matching"])
        self.assertIsNone(claim_matching_scope({"claim_scope": scope}))
        self.assertIn(
            "Claim lacks signal-local place scope and cannot be treated as direct mission evidence yet.",
            direct_matching_gap_for_claim({"claim_scope": scope}),
        )

    def test_observation_tagging_semantics_are_available_from_domain_module(self) -> None:
        mission = smoke_mission()
        plan = build_investigation_plan(mission=mission, round_id="round-01")

        self.assertEqual("air-quality", observation_metric_family("pm25"))
        self.assertEqual(
            ["air-quality", "fire-detection", "meteorology", "hydrology", "soil", "other"],
            claim_priority_metric_families([{"claim_type": "smoke", "needs_physical_validation": True}]),
        )

        tags = infer_observation_investigation_tags(
            {
                "metric": "fire_detection_count",
                "source_skill": "nasa-firms-fire-fetch",
                "place_scope": point_scope("Quebec fire cluster", 49.8, -72.4),
            },
            plan=plan,
            mission_scope=mission["region"],
        )

        self.assertEqual("hypothesis-001", tags["hypothesis_id"])
        self.assertEqual("source", tags["leg_id"])

    def test_metric_assessment_and_summary_semantics_are_directly_importable(self) -> None:
        assessment = assess_observation_against_claim(
            "smoke",
            {
                "metric": "pm2_5",
                "value": 42.0,
                "unit": "ug/m3",
                "evidence_role": "primary",
            },
        )

        self.assertEqual(2, assessment["support_score"])
        self.assertEqual(0, assessment["contradict_score"])
        self.assertEqual(["pm2_5=42"], assessment["notes"])

        summary = build_evidence_summary(
            {"summary": "Canadian wildfire smoke drove the local PM2.5 spike."},
            assessment["notes"],
            "supports",
            [],
        )
        self.assertIn("Matched metrics: pm2_5=42.", summary)


if __name__ == "__main__":
    unittest.main()
