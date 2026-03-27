from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eco_council_runtime.investigation import (  # noqa: E402
    build_investigation_plan,
    causal_focus_for_role,
)


class InvestigationPlanningTests(unittest.TestCase):
    def test_smoke_transport_plan_exposes_rich_leg_metadata(self) -> None:
        mission = {
            "run_id": "run-001",
            "topic": "New York air quality smoke episode",
            "objective": "Assess whether Canadian wildfire smoke drove the New York PM2.5 spike.",
            "hypotheses": ["Canadian wildfire smoke caused the New York AQI surge."],
        }

        plan = build_investigation_plan(mission=mission, round_id="round-01")

        self.assertEqual("smoke-transport", plan["profile_id"])
        legs = {leg["leg_id"]: leg for leg in plan["hypotheses"][0]["chain_legs"]}
        self.assertEqual("derived-source-region", legs["source"]["region_hint"])
        self.assertEqual(["fire-detection"], legs["source"]["metric_families"])
        self.assertEqual(["environmentalist", "moderator"], legs["source"]["preferred_roles"])
        self.assertIn("plume movement", legs["mechanism"]["query_cues"])
        self.assertEqual(["air-quality"], legs["impact"]["metric_families"])
        self.assertIn("wildfire", legs["public_interpretation"]["claim_types"])
        self.assertTrue(legs["impact"]["success_criteria"])

    def test_causal_focus_prioritizes_legs_by_role(self) -> None:
        mission = {
            "run_id": "run-001",
            "topic": "New York air quality smoke episode",
            "objective": "Assess whether Canadian wildfire smoke drove the New York PM2.5 spike.",
            "hypotheses": ["Canadian wildfire smoke caused the New York AQI surge."],
        }
        plan = build_investigation_plan(mission=mission, round_id="round-01")

        sociologist_focus = causal_focus_for_role(plan, "sociologist")
        environmentalist_focus = causal_focus_for_role(plan, "environmentalist")

        self.assertEqual(
            ["public_interpretation"],
            [leg["leg_id"] for leg in sociologist_focus["hypotheses"][0]["priority_legs"]],
        )
        self.assertEqual(
            ["source", "mechanism", "impact"],
            [leg["leg_id"] for leg in environmentalist_focus["hypotheses"][0]["priority_legs"]],
        )
        self.assertIn("wildfire smoke", sociologist_focus["query_cues"])
        self.assertEqual(
            ["fire-detection", "meteorology", "air-quality"],
            environmentalist_focus["priority_metric_families"],
        )


if __name__ == "__main__":
    unittest.main()
