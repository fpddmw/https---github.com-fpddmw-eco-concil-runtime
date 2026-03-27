from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.application.normalize_views import (  # noqa: E402
    build_environment_signal_summary,
    build_public_signal_summary,
    claims_from_submissions,
    compact_claim,
    compact_distribution_summary,
    compact_observation,
    compact_statistics,
    observations_from_submissions,
    ordered_context_observations,
    representative_observation_order,
    representative_observation_submissions,
)


class NormalizeViewsTests(unittest.TestCase):
    def test_compact_builders_are_directly_importable(self) -> None:
        compacted_stats = compact_statistics({"sample_count": 3, "mean": 12.3456, "max": 20})
        self.assertEqual({"sample_count": 3, "mean": 12.346, "max": 20, "min": None, "p05": None, "p25": None, "median": None, "p75": None, "p95": None, "stddev": None}, compacted_stats)

        compacted_distribution = compact_distribution_summary(
            {
                "signal_count": 4,
                "distinct_day_count": 2,
                "source_skill_counts": [
                    {"value": "openaq-data-fetch", "count": 3},
                    {"value": "", "count": 99},
                ],
            }
        )
        self.assertEqual(
            {
                "signal_count": 4,
                "distinct_day_count": 2,
                "source_skill_counts": [{"value": "openaq-data-fetch", "count": 3}],
            },
            compacted_distribution,
        )

        claim = compact_claim(
            {
                "claim_id": "claim-001",
                "claim_type": "smoke",
                "summary": "Canadian wildfire smoke affected New York City.",
                "priority": 1,
                "needs_physical_validation": True,
                "hypothesis_id": "hypothesis-001",
                "leg_id": "impact",
                "public_refs": [{"source_skill": "youtube-video-search"}],
                "claim_scope": {"usable_for_matching": True, "place_source": "signal-derived", "time_source": "signal-derived"},
            }
        )
        self.assertEqual("claim-001", claim["claim_id"])
        self.assertEqual(["youtube-video-search"], claim["public_source_skills"])
        self.assertEqual("hypothesis-001", claim["hypothesis_id"])

        observation = compact_observation(
            {
                "observation_id": "obs-001",
                "source_skill": "openaq-data-fetch",
                "metric": "pm2_5",
                "aggregation": "point",
                "value": 41.5,
                "unit": "ug/m3",
                "statistics": {"sample_count": 1, "mean": 41.5},
                "distribution_summary": {"signal_count": 1},
                "quality_flags": ["station-observation", "preliminary"],
            }
        )
        self.assertEqual("air-quality", observation["metric_family"])
        self.assertEqual({"sample_count": 1, "mean": 41.5, "min": None, "p05": None, "p25": None, "median": None, "p75": None, "p95": None, "max": None, "stddev": None}, observation["statistics"])
        self.assertEqual({"signal_count": 1}, observation["distribution_summary"])

    def test_representative_ordering_helpers_are_directly_importable(self) -> None:
        claims = [{"claim_type": "smoke", "needs_physical_validation": True}]
        observations = [
            {"observation_id": "obs-wind", "metric": "wind_speed", "value": 5.0, "source_skill": "open-meteo-historical-fetch"},
            {"observation_id": "obs-pm", "metric": "pm2_5", "value": 41.5, "source_skill": "openaq-data-fetch"},
            {"observation_id": "obs-fire", "metric": "fire_detection_count", "value": 8.0, "source_skill": "nasa-firms-fire-fetch"},
        ]

        ordered = representative_observation_order(observations, claims)
        self.assertEqual(["obs-pm", "obs-fire", "obs-wind"], [item["observation_id"] for item in ordered])

        context_order = ordered_context_observations(
            observations,
            [{"observation_ids": ["obs-wind"]}],
            claims=claims,
        )
        self.assertEqual(["obs-wind", "obs-pm", "obs-fire"], [item["observation_id"] for item in context_order])

    def test_submission_materializers_and_summary_builders_are_directly_importable(self) -> None:
        validation_calls: list[tuple[str, str]] = []
        claim_submissions = [
            {
                "run_id": "run-001",
                "round_id": "round-01",
                "claim_type": "smoke",
                "summary": "Smoke impacted NYC",
                "statement": "Smoke impacted NYC.",
                "priority": 2,
                "needs_physical_validation": True,
                "public_refs": [{"source_skill": "youtube-video-search"}],
            }
        ]
        observation_submissions = [
            {
                "run_id": "run-001",
                "round_id": "round-01",
                "observation_id": "obs-b",
                "source_skill": "open-meteo-historical-fetch",
                "metric": "wind_speed",
                "aggregation": "point",
                "value": 5.0,
                "unit": "m/s",
            },
            {
                "run_id": "run-001",
                "round_id": "round-01",
                "observation_id": "obs-a",
                "source_skill": "openaq-data-fetch",
                "metric": "pm2_5",
                "aggregation": "point",
                "value": 41.5,
                "unit": "ug/m3",
            },
        ]

        claims = claims_from_submissions(
            claim_submissions,
            validate_payload=lambda kind, payload: validation_calls.append((kind, payload["claim_id"])),
        )
        observations = observations_from_submissions(
            observation_submissions,
            validate_payload=lambda kind, payload: validation_calls.append((kind, payload["observation_id"])),
        )
        representative_submissions = representative_observation_submissions(
            observation_submissions,
            claims,
        )

        self.assertEqual("claim-001", claims[0]["claim_id"])
        self.assertEqual(["obs-b", "obs-a"], [item["observation_id"] for item in observations])
        self.assertEqual(["obs-a", "obs-b"], [item["observation_id"] for item in representative_submissions])
        self.assertIn(("claim", "claim-001"), validation_calls)
        self.assertIn(("observation", "obs-a"), validation_calls)

        public_summary = build_public_signal_summary(
            [{"signal_id": "pubsig-001", "source_skill": "youtube-video-search", "signal_kind": "video", "title": "Smoke video", "published_at_utc": "2023-06-07T12:00:00Z"}],
            claims,
            generated_at_utc="2026-03-27T00:00:00Z",
        )
        environment_summary = build_environment_signal_summary(
            [{"source_skill": "openaq-data-fetch", "metric": "pm2_5"}],
            observations,
            generated_at_utc="2026-03-27T00:00:00Z",
        )

        self.assertEqual(1, public_summary["signal_count"])
        self.assertEqual(1, public_summary["claim_count"])
        self.assertEqual("claim-001", public_summary["claims"][0]["claim_id"])
        self.assertEqual(1, environment_summary["signal_count"])
        self.assertEqual(2, environment_summary["observation_count"])
        self.assertEqual("obs-a", environment_summary["top_observations"][0]["observation_id"])


if __name__ == "__main__":
    unittest.main()
