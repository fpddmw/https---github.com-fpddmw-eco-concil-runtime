from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.investigation import build_investigation_plan  # noqa: E402
from eco_council_runtime.normalize import (  # noqa: E402
    infer_observation_investigation_tags,
    materialize_observation_submission_from_curated_entry,
    shared_observation_id as normalize_shared_observation_id,
)
from eco_council_runtime.reporting import (  # noqa: E402
    build_hypothesis_review_from_state,
    hydrate_observation_submissions_with_observations,
    shared_observation_id as reporting_shared_observation_id,
)


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


def provenance(source_skill: str, artifact_name: str) -> dict[str, str]:
    return {
        "source_skill": source_skill,
        "artifact_path": f"runs/example/{artifact_name}.json",
    }


def observation_record(
    *,
    observation_id: str,
    source_skill: str,
    metric: str,
    value: float,
    unit: str,
    place_scope: dict[str, object],
    hypothesis_id: str = "",
    leg_id: str = "",
    evidence_role: str = "primary",
    component_roles: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    record: dict[str, object] = {
        "observation_id": observation_id,
        "source_skill": source_skill,
        "metric": metric,
        "aggregation": "point",
        "value": value,
        "unit": unit,
        "time_window": {
            "start_utc": "2023-06-07T00:00:00Z",
            "end_utc": "2023-06-07T23:59:59Z",
        },
        "place_scope": place_scope,
        "quality_flags": [],
        "provenance": provenance(source_skill, observation_id),
        "evidence_role": evidence_role,
    }
    if hypothesis_id:
        record["hypothesis_id"] = hypothesis_id
    if leg_id:
        record["leg_id"] = leg_id
    if component_roles is not None:
        record["component_roles"] = component_roles
    return record


class ObservationLeggingTests(unittest.TestCase):
    def test_smoke_transport_observations_infer_source_mechanism_and_impact(self) -> None:
        mission = smoke_mission()
        plan = build_investigation_plan(mission=mission, round_id="round-01")
        mission_scope = mission["region"]

        source_tags = infer_observation_investigation_tags(
            {
                "metric": "fire_detection_count",
                "source_skill": "nasa-firms-fire-fetch",
                "place_scope": point_scope("Quebec fire cluster", 49.8, -72.4),
            },
            plan=plan,
            mission_scope=mission_scope,
        )
        mechanism_tags = infer_observation_investigation_tags(
            {
                "metric": "wind_speed_10m",
                "source_skill": "open-meteo-historical-fetch",
                "place_scope": point_scope("Ottawa valley corridor", 45.2, -75.8),
            },
            plan=plan,
            mission_scope=mission_scope,
        )
        impact_tags = infer_observation_investigation_tags(
            {
                "metric": "pm2_5",
                "source_skill": "openaq-data-fetch",
                "place_scope": mission_scope,
            },
            plan=plan,
            mission_scope=mission_scope,
        )

        self.assertEqual("hypothesis-001", source_tags["hypothesis_id"])
        self.assertEqual("source", source_tags["leg_id"])
        self.assertEqual("hypothesis-001", mechanism_tags["hypothesis_id"])
        self.assertEqual("mechanism", mechanism_tags["leg_id"])
        self.assertEqual("hypothesis-001", impact_tags["hypothesis_id"])
        self.assertEqual("impact", impact_tags["leg_id"])

    def test_curated_composite_submission_keeps_component_leg_tags_without_forcing_top_level_leg(self) -> None:
        mission = smoke_mission()
        mission_scope = mission["region"]
        mission_time_window = mission["window"]
        candidate_lookup = {
            "obs-source": observation_record(
                observation_id="obs-source",
                source_skill="nasa-firms-fire-fetch",
                metric="fire_detection_count",
                value=12.0,
                unit="count",
                place_scope=point_scope("Quebec fire cluster", 49.8, -72.4),
                hypothesis_id="hypothesis-001",
                leg_id="source",
                evidence_role="contextual",
            ),
            "obs-impact": observation_record(
                observation_id="obs-impact",
                source_skill="openaq-data-fetch",
                metric="pm2_5",
                value=42.0,
                unit="ug/m3",
                place_scope=mission_scope,
                hypothesis_id="hypothesis-001",
                leg_id="impact",
            ),
        }
        entry = {
            "observation_id": "obs-composite-001",
            "observation_mode": "composite",
            "candidate_observation_ids": ["obs-source", "obs-impact"],
            "metric": "smoke_transport_bundle",
            "aggregation": "composite",
            "unit": "mixed",
            "meaning": "Composite smoke transport context spanning source activity and receptor impact.",
            "worth_storing": True,
            "evidence_role": "mixed",
            "selection_reason": "Preserve both source fire evidence and receptor PM2.5 impact in one auditable composite.",
            "component_roles": [
                {
                    "candidate_observation_id": "obs-source",
                    "role": "contextual",
                    "metric": "fire_detection_count",
                    "value": 12.0,
                    "unit": "count",
                },
                {
                    "candidate_observation_id": "obs-impact",
                    "role": "primary",
                    "metric": "pm2_5",
                    "value": 42.0,
                    "unit": "ug/m3",
                },
            ],
        }

        submission = materialize_observation_submission_from_curated_entry(
            entry=entry,
            candidate_lookup=candidate_lookup,
            run_id=mission["run_id"],
            round_id="round-01",
            mission_scope=mission_scope,
            mission_time_window=mission_time_window,
        )

        self.assertEqual("hypothesis-001", submission["hypothesis_id"])
        self.assertNotIn("leg_id", submission)
        components = {
            item["candidate_observation_id"]: item
            for item in submission["component_roles"]
        }
        self.assertEqual("source", components["obs-source"]["leg_id"])
        self.assertEqual("impact", components["obs-impact"]["leg_id"])
        self.assertEqual("hypothesis-001", components["obs-source"]["hypothesis_id"])
        self.assertEqual("hypothesis-001", components["obs-impact"]["hypothesis_id"])

    def test_curated_submission_can_override_top_level_leg_without_losing_component_tags(self) -> None:
        mission = smoke_mission()
        mission_scope = mission["region"]
        mission_time_window = mission["window"]
        candidate_lookup = {
            "obs-source": observation_record(
                observation_id="obs-source",
                source_skill="nasa-firms-fire-fetch",
                metric="fire_detection_count",
                value=12.0,
                unit="count",
                place_scope=point_scope("Quebec fire cluster", 49.8, -72.4),
                hypothesis_id="hypothesis-001",
                leg_id="source",
            ),
            "obs-impact": observation_record(
                observation_id="obs-impact",
                source_skill="openaq-data-fetch",
                metric="pm2_5",
                value=42.0,
                unit="ug/m3",
                place_scope=mission_scope,
                hypothesis_id="hypothesis-001",
                leg_id="impact",
            ),
        }
        entry = {
            "observation_id": "obs-curated-impact-001",
            "observation_mode": "composite",
            "candidate_observation_ids": ["obs-source", "obs-impact"],
            "metric": "pm2_5",
            "aggregation": "composite",
            "unit": "ug/m3",
            "meaning": "Curated impact summary centered on the receptor-region PM2.5 signal.",
            "worth_storing": True,
            "evidence_role": "primary",
            "selection_reason": "Override the top-level leg so this composite is reviewed as the impact summary.",
            "hypothesis_id": "hypothesis-001",
            "leg_id": "impact",
            "component_roles": [
                {
                    "candidate_observation_id": "obs-source",
                    "role": "contextual",
                },
                {
                    "candidate_observation_id": "obs-impact",
                    "role": "primary",
                },
            ],
        }

        submission = materialize_observation_submission_from_curated_entry(
            entry=entry,
            candidate_lookup=candidate_lookup,
            run_id=mission["run_id"],
            round_id="round-01",
            mission_scope=mission_scope,
            mission_time_window=mission_time_window,
        )

        self.assertEqual("hypothesis-001", submission["hypothesis_id"])
        self.assertEqual("impact", submission["leg_id"])
        components = {
            item["candidate_observation_id"]: item
            for item in submission["component_roles"]
        }
        self.assertEqual("source", components["obs-source"]["leg_id"])
        self.assertEqual("impact", components["obs-impact"]["leg_id"])

    def test_reporting_shared_observation_id_matches_normalize_when_tags_exist(self) -> None:
        observation = observation_record(
            observation_id="obs-impact",
            source_skill="openaq-data-fetch",
            metric="pm2_5",
            value=42.0,
            unit="ug/m3",
            place_scope=smoke_mission()["region"],
            hypothesis_id="hypothesis-001",
            leg_id="impact",
            component_roles=[
                {
                    "candidate_observation_id": "obs-impact-candidate",
                    "role": "primary",
                    "metric": "pm2_5",
                    "value": 42.0,
                    "unit": "ug/m3",
                    "hypothesis_id": "hypothesis-001",
                    "leg_id": "impact",
                }
            ],
        )

        self.assertEqual(
            normalize_shared_observation_id(observation),
            reporting_shared_observation_id(observation),
        )

    def test_investigation_review_prefers_explicit_observation_leg_tags_over_metric_family_fallback(self) -> None:
        mission = smoke_mission()
        plan = build_investigation_plan(mission=mission, round_id="round-01")
        observation = observation_record(
            observation_id="obs-explicit-source",
            source_skill="openaq-data-fetch",
            metric="pm2_5",
            value=42.0,
            unit="ug/m3",
            place_scope=mission["region"],
            hypothesis_id="hypothesis-001",
            leg_id="source",
            component_roles=[
                {
                    "candidate_observation_id": "obs-explicit-source",
                    "role": "primary",
                    "metric": "pm2_5",
                    "value": 42.0,
                    "unit": "ug/m3",
                    "hypothesis_id": "hypothesis-001",
                    "leg_id": "source",
                }
            ],
        )
        submission_without_tags = {
            "submission_id": "obssub-legacy-001",
            "run_id": mission["run_id"],
            "round_id": "round-01",
            "agent_role": "environmentalist",
            "observation_id": "obs-explicit-source",
            "source_skill": "openaq-data-fetch",
            "metric": "pm2_5",
            "aggregation": "point",
            "value": 42.0,
            "unit": "ug/m3",
            "meaning": "Legacy submission before hydration.",
            "worth_storing": True,
            "time_window": observation["time_window"],
            "place_scope": observation["place_scope"],
            "quality_flags": [],
            "provenance": provenance("openaq-data-fetch", "legacy-observation-submission"),
        }
        hydrated_submissions = hydrate_observation_submissions_with_observations(
            [submission_without_tags],
            [observation],
        )

        self.assertEqual("hypothesis-001", hydrated_submissions[0]["hypothesis_id"])
        self.assertEqual("source", hydrated_submissions[0]["leg_id"])

        state = {
            "cards_active": [
                {
                    "evidence_id": "evidence-001",
                    "claim_id": "claim-001",
                    "hypothesis_id": "hypothesis-001",
                    "leg_id": "public_interpretation",
                    "observation_ids": ["obs-explicit-source"],
                }
            ],
            "isolated_active": [],
            "remands_open": [],
            "observations": [observation],
            "claims": [],
            "observation_submissions_current": hydrated_submissions,
            "observation_submissions_auditable": hydrated_submissions,
        }

        review = build_hypothesis_review_from_state(
            state=state,
            hypothesis=plan["hypotheses"][0],
            profile_id=plan["profile_id"],
        )
        legs = {item["leg_id"]: item for item in review["leg_reviews"]}

        self.assertEqual("supported", legs["source"]["status"])
        self.assertIn("card:evidence-001", legs["source"]["evidence_refs"])
        self.assertIn("observation:obs-explicit-source", legs["source"]["evidence_refs"])
        self.assertEqual("unresolved", legs["impact"]["status"])
        self.assertEqual([], legs["impact"]["evidence_refs"])


if __name__ == "__main__":
    unittest.main()
