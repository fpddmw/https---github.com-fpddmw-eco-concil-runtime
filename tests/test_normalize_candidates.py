from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.adapters.filesystem import stable_hash  # noqa: E402
from eco_council_runtime.application.normalize_candidates import (  # noqa: E402
    environment_signals_to_observations,
    observation_group_key,
    public_signals_to_claims,
)
from eco_council_runtime.investigation import build_investigation_plan  # noqa: E402


def mission() -> dict[str, object]:
    return {
        "run_id": "eco-20260327-nyc-smoke-canada-20230606",
        "topic": "New York City air quality discussion and Canadian wildfire smoke verification",
        "objective": "Verify whether wildfire smoke drove the New York City PM2.5 spike.",
        "window": mission_window(),
        "region": mission_scope(),
        "hypotheses": [
            "Canadian wildfire smoke was transported into the New York City metro area and drove the local PM2.5 spike."
        ],
    }


def mission_scope() -> dict[str, object]:
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


class NormalizeCandidatesTests(unittest.TestCase):
    def test_public_signals_to_claims_groups_semantic_duplicates(self) -> None:
        plan = build_investigation_plan(mission=mission(), round_id="round-001")
        signals = [
            {
                "signal_id": "pubsig-001",
                "source_skill": "gdelt-gkg-fetch",
                "signal_kind": "gkg-record",
                "title": "Smoke haze chokes New York City",
                "text": "New York City residents reported severe smoke and poor air quality during the haze episode.",
                "artifact_path": "runs/example/gkg-001.json",
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
            },
            {
                "signal_id": "pubsig-002",
                "source_skill": "gdelt-gkg-fetch",
                "signal_kind": "gkg-record",
                "title": "Smoke haze chokes New York City",
                "text": "New York City residents reported severe smoke and poor air quality during the haze episode.",
                "artifact_path": "runs/example/gkg-002.json",
                "record_locator": "$.downloads[1].gkg[0]",
                "published_at_utc": "2023-06-07T14:30:00Z",
                "raw_json": {
                    "locations": [
                        {
                            "name": "New York City",
                            "latitude": 40.7130,
                            "longitude": -74.0062,
                        }
                    ]
                },
            },
            {
                "signal_id": "pubsig-skip",
                "source_skill": "gdelt-gkg-fetch",
                "signal_kind": "timeline-bin",
                "title": "Coverage bin",
                "text": "Synthetic coverage artifact.",
                "artifact_path": "runs/example/timeline.json",
                "record_locator": "$.timeline[0]",
                "published_at_utc": "2023-06-07T15:00:00Z",
            },
        ]

        claims = public_signals_to_claims(
            run_id="run-001",
            round_id="round-001",
            signals=signals,
            max_claims=5,
            mission_scope=mission_scope(),
            mission_time_window=mission_window(),
            investigation_plan=plan,
        )

        self.assertEqual(1, len(claims))
        claim = claims[0]
        self.assertEqual("claim-001", claim["claim_id"])
        self.assertEqual("smoke", claim["claim_type"])
        self.assertEqual(2, claim["source_signal_count"])
        self.assertEqual("hypothesis-001", claim["hypothesis_id"])
        self.assertTrue(claim["claim_scope"]["usable_for_matching"])
        self.assertEqual(2, len(claim["public_refs"]))
        self.assertEqual(
            ["runs/example/gkg-001.json", "runs/example/gkg-002.json"],
            [item["artifact_path"] for item in claim["public_refs"]],
        )
        self.assertEqual(2, claim["compact_audit"]["retained_count"])
        self.assertEqual(2, claim["compact_audit"]["total_candidate_count"])

    def test_observation_group_key_hashes_mission_scope_when_point_is_missing(self) -> None:
        signal = {
            "source_skill": "openaq-data-fetch",
            "metric": "PM2.5",
        }

        key = observation_group_key(signal, mission_scope())

        self.assertEqual("openaq-data-fetch", key[0])
        self.assertEqual("pm2_5", key[1])
        self.assertEqual(
            stable_hash(json.dumps(mission_scope(), sort_keys=True))[:8],
            key[2],
        )

    def test_environment_signals_to_observations_aggregates_fire_events(self) -> None:
        plan = build_investigation_plan(mission=mission(), round_id="round-001")
        signals = [
            {
                "signal_id": "env-001",
                "source_skill": "nasa-firms-fire-fetch",
                "metric": "fire_detection",
                "value": 1,
                "unit": "flag",
                "latitude": 49.1234,
                "longitude": -123.4567,
                "observed_at_utc": "2023-06-07T01:00:00Z",
                "artifact_path": "runs/example/fire-001.json",
                "record_locator": "$[0]",
                "quality_flags": ["night"],
            },
            {
                "signal_id": "env-002",
                "source_skill": "nasa-firms-fire-fetch",
                "metric": "fire_detection",
                "value": 1,
                "unit": "flag",
                "latitude": 49.1232,
                "longitude": -123.4566,
                "observed_at_utc": "2023-06-07T03:00:00Z",
                "artifact_path": "runs/example/fire-002.json",
                "record_locator": "$[1]",
                "quality_flags": ["low-confidence"],
            },
        ]

        observations = environment_signals_to_observations(
            run_id="run-001",
            round_id="round-001",
            signals=signals,
            extra_observations=[],
            mission_scope=mission_scope(),
            mission_time_window=mission_window(),
            investigation_plan=plan,
        )

        self.assertEqual(1, len(observations))
        observation = observations[0]
        self.assertEqual("obs-001", observation["observation_id"])
        self.assertEqual("fire_detection_count", observation["metric"])
        self.assertEqual("event-count", observation["aggregation"])
        self.assertEqual(2.0, observation["value"])
        self.assertEqual("count", observation["unit"])
        self.assertEqual(2, observation["statistics"]["sample_count"])
        self.assertEqual("2023-06-07T01:00:00Z", observation["time_window"]["start_utc"])
        self.assertEqual("2023-06-07T03:00:00Z", observation["time_window"]["end_utc"])
        self.assertEqual(["low-confidence", "night"], observation["quality_flags"])
        self.assertEqual("Point", observation["place_scope"]["geometry"]["type"])
        self.assertAlmostEqual(49.1233, observation["place_scope"]["geometry"]["latitude"], places=4)
        self.assertAlmostEqual(-123.45665, observation["place_scope"]["geometry"]["longitude"], places=4)
        self.assertEqual(2, observation["distribution_summary"]["signal_count"])
        self.assertEqual(
            [{"value": "fire_detection_count", "count": 2}],
            observation["distribution_summary"]["metric_counts"],
        )

    def test_environment_signals_to_observations_fills_extra_observation_defaults(self) -> None:
        plan = build_investigation_plan(mission=mission(), round_id="round-001")
        extra_observations = [
            {
                "schema_version": "1.0.0",
                "source_skill": "manual-curated-observation",
                "metric": "river_discharge",
                "aggregation": "window-summary",
                "value": 120.0,
                "unit": "m3/s",
                "quality_flags": [],
                "provenance": {
                    "source_skill": "manual-curated-observation",
                    "artifact_path": "runs/example/manual.json",
                    "record_locator": "$",
                },
            }
        ]

        observations = environment_signals_to_observations(
            run_id="run-001",
            round_id="round-001",
            signals=[],
            extra_observations=extra_observations,
            mission_scope=mission_scope(),
            mission_time_window=mission_window(),
            investigation_plan=plan,
        )

        self.assertEqual(1, len(observations))
        observation = observations[0]
        self.assertEqual("obs-001", observation["observation_id"])
        self.assertEqual("run-001", observation["run_id"])
        self.assertEqual("round-001", observation["round_id"])
        self.assertEqual(mission_scope(), observation["place_scope"])
        self.assertEqual(mission_window(), observation["time_window"])
        self.assertEqual(1, observation["distribution_summary"]["signal_count"])
        self.assertEqual(
            [{"value": "river_discharge", "count": 1}],
            observation["distribution_summary"]["metric_counts"],
        )
        self.assertIn(
            "extra deterministic observation source",
            observation["compact_audit"]["coverage_summary"],
        )


if __name__ == "__main__":
    unittest.main()
