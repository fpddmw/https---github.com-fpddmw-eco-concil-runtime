from __future__ import annotations

import sys
import unittest
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.investigation import build_investigation_plan  # noqa: E402
from eco_council_runtime.normalize import (  # noqa: E402
    build_evidence_cards_from_matches,
    build_isolated_entries,
    build_matching_result,
    claim_submission_from_claim,
    infer_observation_investigation_tags,
    match_claims_to_observations,
    public_signals_to_claims,
)
from eco_council_runtime.reporting import build_hypothesis_review_from_state  # noqa: E402


ROUND_ID = "round-01"


def bbox_scope(label: str, west: float, south: float, east: float, north: float) -> dict[str, object]:
    return {
        "label": label,
        "geometry": {
            "type": "BBox",
            "west": west,
            "south": south,
            "east": east,
            "north": north,
        },
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


def smoke_mission() -> dict[str, object]:
    return {
        "run_id": "eco-benchmark-smoke",
        "topic": "New York City smoke haze and air quality",
        "objective": "Assess whether Canadian smoke caused the New York City PM2.5 spike.",
        "window": {
            "start_utc": "2023-06-06T00:00:00Z",
            "end_utc": "2023-06-08T23:59:59Z",
        },
        "region": bbox_scope("New York City metro area, USA", -74.30, 40.45, -73.65, 40.95),
        "hypotheses": [
            "Canadian wildfire smoke was transported into the New York City metro area and drove the local PM2.5 spike."
        ],
    }


def flood_mission() -> dict[str, object]:
    return {
        "run_id": "eco-benchmark-flood",
        "topic": "St. Louis river flooding verification",
        "objective": "Assess whether upstream rainfall and river transfer drove flooding in St. Louis.",
        "window": {
            "start_utc": "2024-04-02T00:00:00Z",
            "end_utc": "2024-04-05T23:59:59Z",
        },
        "region": bbox_scope("St. Louis metro area, USA", -90.45, 38.45, -90.05, 38.85),
        "hypotheses": [
            "Upstream rainfall and river transfer caused the flood impact in the St. Louis metro area."
        ],
    }


def heat_mission() -> dict[str, object]:
    return {
        "run_id": "eco-benchmark-heat",
        "topic": "Phoenix extreme heat verification",
        "objective": "Assess whether extreme heat caused dangerous heat stress across Phoenix.",
        "window": {
            "start_utc": "2024-07-10T00:00:00Z",
            "end_utc": "2024-07-12T23:59:59Z",
        },
        "region": bbox_scope("Phoenix metro area, USA", -112.35, 33.20, -111.85, 33.75),
        "hypotheses": [
            "Extreme heat caused dangerous local heat stress across the Phoenix metro area."
        ],
    }


def policy_mission() -> dict[str, object]:
    return {
        "run_id": "eco-benchmark-policy",
        "topic": "Newark environmental rulemaking reaction",
        "objective": "Assess local public reaction to the EPA environmental rulemaking in Newark.",
        "window": {
            "start_utc": "2025-02-10T00:00:00Z",
            "end_utc": "2025-02-14T23:59:59Z",
        },
        "region": bbox_scope("Newark, New Jersey, USA", -74.30, 40.60, -74.05, 40.82),
        "hypotheses": [
            "Local public commenters in Newark reacted to the EPA environmental rulemaking."
        ],
    }


def localized_signal(
    *,
    signal_id: str,
    source_skill: str,
    title: str,
    text: str,
    latitude: float,
    longitude: float,
    location_label: str,
    published_at_utc: str,
) -> dict[str, object]:
    return {
        "signal_id": signal_id,
        "source_skill": source_skill,
        "signal_kind": "gkg-record",
        "title": title,
        "text": text,
        "artifact_path": f"runs/benchmarks/{signal_id}.json",
        "record_locator": "$.records[0]",
        "published_at_utc": published_at_utc,
        "raw_json": {
            "locations": [
                {
                    "name": location_label,
                    "latitude": latitude,
                    "longitude": longitude,
                }
            ]
        },
    }


def unlocalized_signal(
    *,
    signal_id: str,
    source_skill: str,
    title: str,
    text: str,
    published_at_utc: str,
) -> dict[str, object]:
    return {
        "signal_id": signal_id,
        "source_skill": source_skill,
        "signal_kind": "post",
        "title": title,
        "text": text,
        "artifact_path": f"runs/benchmarks/{signal_id}.json",
        "record_locator": "$.records[0]",
        "published_at_utc": published_at_utc,
    }


def observation_record(
    *,
    mission: dict[str, object],
    observation_id: str,
    metric: str,
    value: float,
    unit: str,
    place_scope: dict[str, object],
    source_skill: str,
) -> dict[str, object]:
    return {
        "schema_version": "1.0.0",
        "observation_id": observation_id,
        "run_id": mission["run_id"],
        "round_id": ROUND_ID,
        "agent_role": "environmentalist",
        "source_skill": source_skill,
        "metric": metric,
        "aggregation": "point",
        "value": value,
        "unit": unit,
        "time_window": mission["window"],
        "place_scope": place_scope,
        "quality_flags": [],
        "provenance": {
            "source_skill": source_skill,
            "artifact_path": f"runs/benchmarks/{observation_id}.json",
        },
    }


BENCHMARK_CASES: list[dict[str, Any]] = [
    {
        "case_id": "smoke_support",
        "mission": smoke_mission(),
        "signals": [
            localized_signal(
                signal_id="pubsig-smoke-support",
                source_skill="gdelt-gkg-fetch",
                title="Smoke haze hits New York City",
                text="New York City residents reported dense smoke haze and poor air quality during the episode.",
                latitude=40.7128,
                longitude=-74.0060,
                location_label="New York City",
                published_at_utc="2023-06-07T13:00:00Z",
            )
        ],
        "observations": [
            observation_record(
                mission=smoke_mission(),
                observation_id="obs-smoke-source",
                metric="fire_detection_count",
                value=9.0,
                unit="count",
                place_scope=point_scope("Quebec fire cluster", 49.8, -72.4),
                source_skill="nasa-firms-fire-fetch",
            ),
            observation_record(
                mission=smoke_mission(),
                observation_id="obs-smoke-mechanism",
                metric="wind_speed_10m",
                value=11.0,
                unit="m/s",
                place_scope=point_scope("Ottawa Valley corridor", 45.2, -75.8),
                source_skill="open-meteo-historical-fetch",
            ),
            observation_record(
                mission=smoke_mission(),
                observation_id="obs-smoke-impact",
                metric="pm2_5",
                value=68.0,
                unit="ug/m3",
                place_scope=smoke_mission()["region"],
                source_skill="openaq-data-fetch",
            ),
        ],
        "expected": {
            "profile_id": "smoke-transport",
            "claim_type": "smoke",
            "claim_scope_usable": True,
            "match_verdict": "supports",
            "result_status": "partial",
            "matched_metrics": ["pm2_5"],
            "expect_pair": True,
            "observation_legs": {
                "obs-smoke-source": "source",
                "obs-smoke-mechanism": "mechanism",
                "obs-smoke-impact": "impact",
            },
            "review_leg_statuses": {
                "source": "partial",
                "mechanism": "partial",
                "impact": "supported",
                "public_interpretation": "supported",
            },
            "review_overall_status": "partial",
        },
    },
    {
        "case_id": "smoke_no_local_impact",
        "mission": smoke_mission(),
        "signals": [
            localized_signal(
                signal_id="pubsig-smoke-negative",
                source_skill="gdelt-gkg-fetch",
                title="Smoke haze worries New York City residents",
                text="New York City residents described smoke haze and reduced visibility during the event.",
                latitude=40.7128,
                longitude=-74.0060,
                location_label="New York City",
                published_at_utc="2023-06-07T14:00:00Z",
            )
        ],
        "observations": [
            observation_record(
                mission=smoke_mission(),
                observation_id="obs-smoke-negative-source",
                metric="fire_detection_count",
                value=7.0,
                unit="count",
                place_scope=point_scope("Quebec fire cluster", 49.7, -72.6),
                source_skill="nasa-firms-fire-fetch",
            ),
            observation_record(
                mission=smoke_mission(),
                observation_id="obs-smoke-negative-mechanism",
                metric="wind_speed_10m",
                value=10.0,
                unit="m/s",
                place_scope=point_scope("Ottawa Valley corridor", 45.4, -75.9),
                source_skill="open-meteo-historical-fetch",
            ),
        ],
        "expected": {
            "profile_id": "smoke-transport",
            "claim_type": "smoke",
            "claim_scope_usable": True,
            "match_verdict": "insufficient",
            "result_status": "unmatched",
            "matched_metrics": [],
            "expect_pair": False,
            "observation_legs": {
                "obs-smoke-negative-source": "source",
                "obs-smoke-negative-mechanism": "mechanism",
            },
            "gaps_contain": [
                "No observations matched the claim's localized window and geometry.",
            ],
            "review_leg_statuses": {
                "source": "partial",
                "mechanism": "partial",
                "impact": "unresolved",
                "public_interpretation": "partial",
            },
            "review_overall_status": "partial",
        },
    },
    {
        "case_id": "smoke_ambiguous_attribution",
        "mission": smoke_mission(),
        "signals": [
            localized_signal(
                signal_id="pubsig-smoke-ambiguous",
                source_skill="gdelt-gkg-fetch",
                title="Smoke haze blankets New York City",
                text="New York City residents reported dense smoke haze and degraded air quality during the event.",
                latitude=40.7128,
                longitude=-74.0060,
                location_label="New York City",
                published_at_utc="2023-06-07T13:30:00Z",
            )
        ],
        "observations": [
            observation_record(
                mission=smoke_mission(),
                observation_id="obs-smoke-ambiguous-impact",
                metric="pm2_5",
                value=61.0,
                unit="ug/m3",
                place_scope=smoke_mission()["region"],
                source_skill="openaq-data-fetch",
            )
        ],
        "expected": {
            "profile_id": "smoke-transport",
            "claim_type": "smoke",
            "claim_scope_usable": True,
            "match_verdict": "supports",
            "result_status": "matched",
            "matched_metrics": ["pm2_5"],
            "expect_pair": True,
            "observation_legs": {
                "obs-smoke-ambiguous-impact": "impact",
            },
            "review_leg_statuses": {
                "source": "unresolved",
                "mechanism": "unresolved",
                "impact": "supported",
                "public_interpretation": "supported",
            },
            "review_overall_status": "partial",
        },
    },
    {
        "case_id": "smoke_irrelevant_hydrology",
        "mission": smoke_mission(),
        "signals": [
            localized_signal(
                signal_id="pubsig-smoke-irrelevant",
                source_skill="gdelt-gkg-fetch",
                title="Smoke haze worries New York City residents",
                text="New York City residents described smoke haze and poor air quality during the episode.",
                latitude=40.7128,
                longitude=-74.0060,
                location_label="New York City",
                published_at_utc="2023-06-07T14:30:00Z",
            )
        ],
        "observations": [
            observation_record(
                mission=smoke_mission(),
                observation_id="obs-smoke-hydrology",
                metric="river_discharge",
                value=200.0,
                unit="m3/s",
                place_scope=smoke_mission()["region"],
                source_skill="usgs-water-iv-fetch",
            )
        ],
        "expected": {
            "profile_id": "smoke-transport",
            "claim_type": "smoke",
            "claim_scope_usable": True,
            "match_verdict": "insufficient",
            "result_status": "unmatched",
            "matched_metrics": [],
            "expect_pair": False,
            "gaps_contain": [
                "No observations matched the claim's localized window and geometry.",
                "Station-grade corroboration is missing.",
            ],
            "review_leg_statuses": {
                "source": "unresolved",
                "mechanism": "unresolved",
                "impact": "unresolved",
                "public_interpretation": "partial",
            },
            "review_overall_status": "unresolved",
        },
    },
    {
        "case_id": "flood_support",
        "mission": flood_mission(),
        "signals": [
            localized_signal(
                signal_id="pubsig-flood-support",
                source_skill="gdelt-doc-search",
                title="St. Louis flooding closes riverside roads",
                text="St. Louis authorities reported flood impacts and rising river levels during the storm sequence.",
                latitude=38.6270,
                longitude=-90.1994,
                location_label="St. Louis",
                published_at_utc="2024-04-04T11:30:00Z",
            )
        ],
        "observations": [
            observation_record(
                mission=flood_mission(),
                observation_id="obs-flood-source",
                metric="precipitation_sum",
                value=38.0,
                unit="mm",
                place_scope=point_scope("Upstream rainfall sector", 39.5, -91.6),
                source_skill="open-meteo-historical-fetch",
            ),
            observation_record(
                mission=flood_mission(),
                observation_id="obs-flood-mechanism",
                metric="river_discharge",
                value=165.0,
                unit="m3/s",
                place_scope=point_scope("Missouri River transfer corridor", 39.0, -91.0),
                source_skill="usgs-water-iv-fetch",
            ),
            observation_record(
                mission=flood_mission(),
                observation_id="obs-flood-impact",
                metric="river_discharge",
                value=210.0,
                unit="m3/s",
                place_scope=flood_mission()["region"],
                source_skill="usgs-water-iv-fetch",
            ),
        ],
        "expected": {
            "profile_id": "flood-upstream",
            "claim_type": "flood",
            "claim_scope_usable": True,
            "match_verdict": "supports",
            "result_status": "partial",
            "matched_metrics": ["river_discharge"],
            "expect_pair": True,
            "observation_legs": {
                "obs-flood-source": "source",
                "obs-flood-mechanism": "mechanism",
                "obs-flood-impact": "impact",
            },
            "review_leg_statuses": {
                "source": "partial",
                "mechanism": "partial",
                "impact": "supported",
                "public_interpretation": "supported",
            },
            "review_overall_status": "partial",
        },
    },
    {
        "case_id": "flood_no_local_impact",
        "mission": flood_mission(),
        "signals": [
            localized_signal(
                signal_id="pubsig-flood-negative",
                source_skill="gdelt-doc-search",
                title="St. Louis residents worry about flood risk",
                text="St. Louis residents discussed flood danger as upstream waters rose.",
                latitude=38.6270,
                longitude=-90.1994,
                location_label="St. Louis",
                published_at_utc="2024-04-04T12:00:00Z",
            )
        ],
        "observations": [
            observation_record(
                mission=flood_mission(),
                observation_id="obs-flood-negative-source",
                metric="precipitation_sum",
                value=32.0,
                unit="mm",
                place_scope=point_scope("Upstream rainfall sector", 39.6, -91.4),
                source_skill="open-meteo-historical-fetch",
            ),
            observation_record(
                mission=flood_mission(),
                observation_id="obs-flood-negative-mechanism",
                metric="river_discharge",
                value=150.0,
                unit="m3/s",
                place_scope=point_scope("Missouri River transfer corridor", 39.1, -91.1),
                source_skill="usgs-water-iv-fetch",
            ),
        ],
        "expected": {
            "profile_id": "flood-upstream",
            "claim_type": "flood",
            "claim_scope_usable": True,
            "match_verdict": "insufficient",
            "result_status": "unmatched",
            "matched_metrics": [],
            "expect_pair": False,
            "observation_legs": {
                "obs-flood-negative-source": "source",
                "obs-flood-negative-mechanism": "mechanism",
            },
            "gaps_contain": [
                "No observations matched the claim's localized window and geometry.",
            ],
            "review_leg_statuses": {
                "source": "partial",
                "mechanism": "partial",
                "impact": "unresolved",
                "public_interpretation": "partial",
            },
            "review_overall_status": "partial",
        },
    },
    {
        "case_id": "heat_support",
        "mission": heat_mission(),
        "signals": [
            localized_signal(
                signal_id="pubsig-heat-support",
                source_skill="gdelt-doc-search",
                title="Phoenix residents struggle through extreme heat",
                text="Phoenix residents reported extreme heat and dangerous heat stress across the metro area.",
                latitude=33.4484,
                longitude=-112.0740,
                location_label="Phoenix",
                published_at_utc="2024-07-11T18:00:00Z",
            )
        ],
        "observations": [
            observation_record(
                mission=heat_mission(),
                observation_id="obs-heat-impact",
                metric="temperature_2m",
                value=42.5,
                unit="c",
                place_scope=heat_mission()["region"],
                source_skill="open-meteo-historical-fetch",
            )
        ],
        "expected": {
            "profile_id": "local-event",
            "claim_type": "heat",
            "claim_scope_usable": True,
            "match_verdict": "supports",
            "result_status": "matched",
            "matched_metrics": ["temperature_2m"],
            "expect_pair": True,
            "observation_legs": {
                "obs-heat-impact": "impact",
            },
            "review_leg_statuses": {
                "impact": "supported",
                "public_interpretation": "supported",
            },
            "review_overall_status": "supported",
        },
    },
    {
        "case_id": "heat_contradiction",
        "mission": heat_mission(),
        "signals": [
            localized_signal(
                signal_id="pubsig-heat-negative",
                source_skill="gdelt-doc-search",
                title="Phoenix residents warn of dangerous heat",
                text="Phoenix residents warned that extreme heat was creating dangerous local conditions.",
                latitude=33.4484,
                longitude=-112.0740,
                location_label="Phoenix",
                published_at_utc="2024-07-11T18:15:00Z",
            )
        ],
        "observations": [
            observation_record(
                mission=heat_mission(),
                observation_id="obs-heat-contradict",
                metric="temperature_2m",
                value=20.0,
                unit="c",
                place_scope=heat_mission()["region"],
                source_skill="open-meteo-historical-fetch",
            )
        ],
        "expected": {
            "profile_id": "local-event",
            "claim_type": "heat",
            "claim_scope_usable": True,
            "match_verdict": "contradicts",
            "result_status": "matched",
            "matched_metrics": ["temperature_2m"],
            "expect_pair": True,
            "observation_legs": {
                "obs-heat-contradict": "impact",
            },
        },
    },
    {
        "case_id": "policy_localized_public_only",
        "mission": policy_mission(),
        "signals": [
            localized_signal(
                signal_id="pubsig-policy-support",
                source_skill="regulationsgov-comments-fetch",
                title="Newark residents file EPA rulemaking comments",
                text="Residents in Newark submitted public comment letters about the EPA environmental rulemaking.",
                latitude=40.7357,
                longitude=-74.1724,
                location_label="Newark",
                published_at_utc="2025-02-12T16:00:00Z",
            )
        ],
        "observations": [],
        "expected": {
            "profile_id": "local-event",
            "claim_type": "policy-reaction",
            "claim_scope_usable": True,
            "match_verdict": "insufficient",
            "result_status": "unmatched",
            "matched_metrics": [],
            "expect_pair": False,
            "review_leg_statuses": {
                "impact": "unresolved",
                "public_interpretation": "partial",
            },
            "review_overall_status": "unresolved",
            "needs_physical_validation": False,
        },
    },
    {
        "case_id": "policy_unlocalized_public_only",
        "mission": policy_mission(),
        "signals": [
            unlocalized_signal(
                signal_id="pubsig-policy-negative",
                source_skill="regulationsgov-comments-fetch",
                title="Residents file EPA rulemaking comments",
                text="Public comment letters discussed the EPA environmental rulemaking and agency process.",
                published_at_utc="2025-02-12T16:15:00Z",
            )
        ],
        "observations": [],
        "expected": {
            "profile_id": "local-event",
            "claim_type": "policy-reaction",
            "claim_scope_usable": False,
            "match_verdict": "insufficient",
            "result_status": "unmatched",
            "matched_metrics": [],
            "expect_pair": False,
            "gaps_contain": [
                "Claim lacks signal-local place scope and cannot be treated as direct mission evidence yet.",
            ],
            "needs_physical_validation": False,
        },
    },
]


def tagged_observations(*, mission: dict[str, Any], observations: list[dict[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    plan = build_investigation_plan(mission=mission, round_id=ROUND_ID)
    tagged: list[dict[str, Any]] = []
    for observation in observations:
        item = dict(observation)
        item.update(
            infer_observation_investigation_tags(
                item,
                plan=plan,
                mission_scope=mission["region"],
            )
        )
        tagged.append(item)
    return plan, tagged


def execute_benchmark_case(case: dict[str, Any]) -> dict[str, Any]:
    mission = case["mission"]
    plan, observations = tagged_observations(mission=mission, observations=case["observations"])
    claims = public_signals_to_claims(
        mission=mission,
        round_id=ROUND_ID,
        signals=case["signals"],
        max_claims=1,
    )
    matches = match_claims_to_observations(
        claims=claims,
        observations=observations,
    )
    matching_result = build_matching_result(
        authorization={
            "run_id": mission["run_id"],
            "round_id": ROUND_ID,
            "authorization_id": f"matchauth-{case['case_id']}",
        },
        claims=claims,
        observations=observations,
        matches=matches,
    )
    cards = build_evidence_cards_from_matches(matches)
    isolated_entries, _ = build_isolated_entries(
        run_id=mission["run_id"],
        round_id=ROUND_ID,
        claims=claims,
        observations=observations,
        matches=matches,
        allow_isolated_evidence=True,
    )
    state = {
        "cards_active": cards,
        "isolated_active": isolated_entries,
        "remands_open": [],
        "observations": observations,
        "claims": claims,
        "claim_submissions_auditable": [claim_submission_from_claim(item) for item in claims],
    }
    review = build_hypothesis_review_from_state(
        state=state,
        hypothesis=plan["hypotheses"][0],
        profile_id=plan["profile_id"],
    )
    return {
        "plan": plan,
        "claims": claims,
        "observations": observations,
        "matches": matches,
        "matching_result": matching_result,
        "cards": cards,
        "review": review,
    }


class InvestigationBenchmarkTests(unittest.TestCase):
    def test_benchmark_matrix_covers_expected_profile_claim_and_match_outcomes(self) -> None:
        for case in BENCHMARK_CASES:
            expected = case["expected"]
            with self.subTest(case=case["case_id"]):
                outcome = execute_benchmark_case(case)

                self.assertEqual(expected["profile_id"], outcome["plan"]["profile_id"])
                self.assertEqual(1, len(outcome["claims"]))
                claim = outcome["claims"][0]
                self.assertEqual(expected["claim_type"], claim["claim_type"])
                self.assertEqual("hypothesis-001", claim["hypothesis_id"])
                self.assertEqual("public_interpretation", claim["leg_id"])
                self.assertEqual(expected["claim_scope_usable"], claim["claim_scope"]["usable_for_matching"])
                if "needs_physical_validation" in expected:
                    self.assertEqual(expected["needs_physical_validation"], claim["needs_physical_validation"])

                self.assertEqual(1, len(outcome["matches"]))
                match = outcome["matches"][0]
                self.assertEqual(expected["match_verdict"], match["verdict"])
                self.assertEqual(expected["matched_metrics"], [item["metric"] for item in match["observations"]])
                for text in expected.get("gaps_contain", []):
                    self.assertIn(text, match["gaps"])

                result = outcome["matching_result"]
                self.assertEqual(expected["result_status"], result["result_status"])
                if expected["expect_pair"]:
                    self.assertEqual(1, len(result["matched_pairs"]))
                    self.assertEqual("hypothesis-001", result["matched_pairs"][0]["hypothesis_id"])
                    self.assertEqual("public_interpretation", result["matched_pairs"][0]["leg_id"])
                    self.assertEqual(1, len(outcome["cards"]))
                    self.assertEqual("hypothesis-001", outcome["cards"][0]["hypothesis_id"])
                    self.assertEqual("public_interpretation", outcome["cards"][0]["leg_id"])
                else:
                    self.assertEqual([], result["matched_pairs"])
                    self.assertEqual([], outcome["cards"])

    def test_benchmark_matrix_preserves_expected_observation_legging(self) -> None:
        for case in BENCHMARK_CASES:
            expected_legs = case["expected"].get("observation_legs", {})
            if not expected_legs:
                continue
            with self.subTest(case=case["case_id"]):
                outcome = execute_benchmark_case(case)
                observations_by_id = {
                    item["observation_id"]: item
                    for item in outcome["observations"]
                }
                for observation_id, leg_id in expected_legs.items():
                    self.assertEqual("hypothesis-001", observations_by_id[observation_id]["hypothesis_id"])
                    self.assertEqual(leg_id, observations_by_id[observation_id]["leg_id"])

    def test_benchmark_matrix_preserves_selected_review_statuses(self) -> None:
        for case in BENCHMARK_CASES:
            expected_statuses = case["expected"].get("review_leg_statuses")
            if not expected_statuses:
                continue
            with self.subTest(case=case["case_id"]):
                outcome = execute_benchmark_case(case)
                review = outcome["review"]
                self.assertEqual(case["expected"]["review_overall_status"], review["overall_status"])
                statuses_by_leg = {
                    item["leg_id"]: item["status"]
                    for item in review["leg_reviews"]
                }
                for leg_id, status in expected_statuses.items():
                    self.assertEqual(status, statuses_by_leg[leg_id])


if __name__ == "__main__":
    unittest.main()
